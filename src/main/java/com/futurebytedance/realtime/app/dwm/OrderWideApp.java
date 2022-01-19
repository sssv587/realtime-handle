package com.futurebytedance.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.futurebytedance.realtime.app.func.DimAsyncFunction;
import com.futurebytedance.realtime.bean.OrderDetail;
import com.futurebytedance.realtime.bean.OrderInfo;
import com.futurebytedance.realtime.bean.OrderWide;
import com.futurebytedance.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2022/1/13 - 23:54
 * @Description 合并订单宽表
 * 业务执行流程
 * -模拟生成数据
 * -数据插入到mysql中
 * -在Binlog中记录数据的变化
 * -Maxwell将数据以Json的形式发送到Kafka的ODS(ods_base_db_m)
 * -BaseDataBaseApp读取ods_base_db_m中的数据进行分流
 * >从Mysql的配置表读取数据
 * >将配置缓存到map集合
 * >检查Phoenix中的表是否存在
 * >对数据进行分流发送到不同的dwd
 */
public class OrderWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 准备本地测试流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //1.3 设置Checkpoint
//        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60000);
//        env.setStateBackend(new FsStateBackend("hdfs://localhost:8020/mall/checkpoint/unique"));

        //TODO 2.从kafka的DWD层读取订单和订单明细数据
        //2.1 声明相关的主题以及消费者组
        String orderInfoSourceTopic = "dwd_order_info";
        String orderDetailSourceTopic = "dwd_order_detail";
        String orderWideSinkTopic = "dwm_order_wide";
        String groupId = "order_wide_group";

        //2.2 读取订单主题数据
        FlinkKafkaConsumer<String> orderInfoSource = MyKafkaUtil.getKafkaSource(orderInfoSourceTopic, groupId);
        DataStreamSource<String> orderInfoDataStream = env.addSource(orderInfoSource);

        //2.3 读取订单明细数据
        FlinkKafkaConsumer<String> orderDetailSource = MyKafkaUtil.getKafkaSource(orderDetailSourceTopic, groupId);
        DataStreamSource<String> orderDetailDataStream = env.addSource(orderDetailSource);

        //TODO 3.对读取的数据进行结构的转换 jsonString --> OrderInfo | OrderDetail
        //3.1 转换订单数据结构
        SingleOutputStreamOperator<OrderInfo> orderInfoMapDataStream = (SingleOutputStreamOperator<OrderInfo>) orderInfoDataStream.map(
                new RichMapFunction<String, OrderInfo>() {
                    SimpleDateFormat simpleDateFormat = null;

                    @Override
                    public void open(Configuration parameters) {
                        simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    }

                    @Override
                    public OrderInfo map(String value) throws Exception {
                        OrderInfo orderInfo = JSON.parseObject(value, OrderInfo.class);
                        orderInfo.setCreate_ts(simpleDateFormat.parse(orderInfo.getCreate_time()).getTime());
                        return orderInfo;
                    }
                }
        );

        //3.2 转换订单明细结构
        SingleOutputStreamOperator<OrderDetail> orderDetailMapDataStream = orderDetailDataStream.map(new RichMapFunction<String, OrderDetail>() {
            SimpleDateFormat simpleDateFormat = null;

            @Override
            public void open(Configuration parameters) {
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }

            @Override
            public OrderDetail map(String value) throws Exception {
                OrderDetail orderDetail = JSON.parseObject(value, OrderDetail.class);
                orderDetail.setCreate_ts(simpleDateFormat.parse(orderDetail.getCreate_time()).getTime());
                return orderDetail;
            }
        });

        //TODO 4.指定事件时间字段
        //4.1 订单指定事件时间字段
        SingleOutputStreamOperator<OrderInfo> orderInfoWithTs = orderInfoMapDataStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<OrderInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderInfo>() {
                            @Override
                            public long extractTimestamp(OrderInfo element, long recordTimestamp) {
                                return element.getCreate_ts();
                            }
                        })
        );

        //4.2 订单明细指定事件时间字段
        SingleOutputStreamOperator<OrderDetail> orderDetailWithTs = orderDetailMapDataStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<OrderDetail>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<OrderDetail>() {
                            @Override
                            public long extractTimestamp(OrderDetail element, long recordTimestamp) {
                                return element.getCreate_ts();
                            }
                        })
        );

        //TODO 5.按照订单id进行分组 指定关联的key
        KeyedStream<OrderInfo, Long> orderInfoKeyedDataStream = orderInfoWithTs.keyBy(OrderInfo::getId);
        KeyedStream<OrderDetail, Long> orderDetailLongKeyedStream = orderDetailWithTs.keyBy(OrderDetail::getOrder_id);

        //TODO 6.使用intervalJoin对订单和订单明细进行关联
        SingleOutputStreamOperator<OrderWide> orderWideDataStream = orderInfoKeyedDataStream
                .intervalJoin(orderDetailLongKeyedStream)
                .between(Time.milliseconds(-5), Time.milliseconds(5))
                .process(new ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>() {
                    @Override
                    public void processElement(OrderInfo left, OrderDetail right, ProcessJoinFunction<OrderInfo, OrderDetail, OrderWide>.Context ctx, Collector<OrderWide> out) {
                        out.collect(new OrderWide(left, right));
                    }
                });

        //TODO 7.关联用户维度
        SingleOutputStreamOperator<OrderWide> orderWideWithUserDataStream = AsyncDataStream.unorderedWait(
                orderWideDataStream,
                new DimAsyncFunction<OrderWide>("DIM_USER_INFO") {
                    @Override
                    public String getKey(OrderWide obj) {
                        return obj.getUser_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJson) throws Exception {
                        //获取用户的生日
                        String birthday = dimInfoJson.getString("BIRTHDAY");

                        //定义日期转换的工具类
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                        //将生日字符串转换为日期对象
                        Date birthdayDate = sdf.parse(birthday);
                        //获取生日日期的毫秒数
                        Long birthdayTs = birthdayDate.getTime();

                        //获取当前时间的毫秒数
                        Long curTs = System.currentTimeMillis();
                        //年龄毫秒数转换为年龄
                        long ageTs = curTs - birthdayTs;
                        //转换为年龄
                        long ageLong = ageTs / 1000L / 60L / 60L / 24L / 365L;
                        int age = (int) ageLong;

                        //将维度中年龄赋值给订单宽表中的字段
                        orderWide.setUser_age(age);
                        //将维度中的性别赋值给订单中的宽表
                        orderWide.setUser_gender(dimInfoJson.getString("GENDER"));
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 8.关联省市维度
        SingleOutputStreamOperator<OrderWide> orderWideWithProvinceDataStream = AsyncDataStream.unorderedWait(
                orderWideWithUserDataStream,
                new DimAsyncFunction<OrderWide>("DIM_BASE_PROVINCE") {

                    @Override
                    public String getKey(OrderWide obj) {
                        return obj.getProvince_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJson) {
                        orderWide.setProvince_name(dimInfoJson.getString("NAME"));
                        orderWide.setProvince_area_code(dimInfoJson.getString("AREA_CODE"));
                        orderWide.setProvince_iso_code(dimInfoJson.getString("ISO_CODE"));
                        orderWide.setProvince_3166_2_code(dimInfoJson.getString("ISO_3166_2"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 9.关联SKU维度
        SingleOutputStreamOperator<OrderWide> orderWideSkuDataStream = AsyncDataStream.unorderedWait(
                orderWideWithProvinceDataStream,
                new DimAsyncFunction<OrderWide>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(OrderWide obj) {
                        return obj.getSku_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJson) {
                        orderWide.setSku_name(dimInfoJson.getString("SKU_NAME"));
                        orderWide.setSpu_id(dimInfoJson.getLong("SPU_ID"));
                        orderWide.setCategory3_id(dimInfoJson.getLong("CATEGORY3_ID"));
                        orderWide.setTm_id(dimInfoJson.getLong("TM_ID"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 10.关联SPU维度
        SingleOutputStreamOperator<OrderWide> orderWideWithSpuDataStream = AsyncDataStream.unorderedWait(
                orderWideSkuDataStream,
                new DimAsyncFunction<OrderWide>("DIM_SPU_INFO") {

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return orderWide.getSpu_id().toString();
                    }

                    @Override
                    public void join(OrderWide orderWide, JSONObject dimInfoJson) {
                        orderWide.setSpu_name(dimInfoJson.getString("SPU_NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 11.关联品类维度
        SingleOutputStreamOperator<OrderWide> orderWideWithCategoryDataStream =
                AsyncDataStream.unorderedWait(
                        orderWideWithSpuDataStream, new DimAsyncFunction<OrderWide>("DIM_BASE_CATEGORY3") {
                            @Override
                            public void join(OrderWide orderWide, JSONObject jsonObject) {
                                orderWide.setCategory3_name(jsonObject.getString("NAME"));
                            }

                            @Override
                            public String getKey(OrderWide orderWide) {
                                return String.valueOf(orderWide.getCategory3_id());
                            }
                        }, 60, TimeUnit.SECONDS);

        //TODO 12.关联品牌维度
        SingleOutputStreamOperator<OrderWide> orderWideWithTmDataStream = AsyncDataStream.unorderedWait(
                orderWideWithCategoryDataStream, new DimAsyncFunction<OrderWide>("DIM_BASE_TRADEMARK") {
                    @Override
                    public void join(OrderWide orderWide, JSONObject jsonObject) throws Exception {
                        orderWide.setTm_name(jsonObject.getString("TM_NAME"));
                    }

                    @Override
                    public String getKey(OrderWide orderWide) {
                        return String.valueOf(orderWide.getTm_id());
                    }
                }, 60, TimeUnit.SECONDS);

        //TODO 13.将关联后的订单宽表数据写回到kafka的DWM层
        orderWideWithTmDataStream.map(JSON::toJSONString).addSink(MyKafkaUtil.getKafkaSink(orderWideSinkTopic));

        env.execute();
    }
}
