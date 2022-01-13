package com.futurebytedance.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.futurebytedance.realtime.bean.OrderDetail;
import com.futurebytedance.realtime.bean.OrderInfo;
import com.futurebytedance.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2022/1/13 - 23:54
 * @Description 合并订单宽表
 * 业务执行流程
 *      -模拟生成数据
 *      -数据插入到mysql中
 *      -在Binlog中记录数据的变化
 *      -Maxwell将数据以Json的形式发送到Kafka的ODS(ods_base_db_m)
 *      -BaseDataBaseApp读取ods_base_db_m中的数据进行分流
 *          >从Mysql的配置表读取数据
 *          >将配置缓存到map集合
 *          >检查Phoenix中的表是否存在
 *          >对数据进行分流发送到不同的dwd
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
                    public void open(Configuration parameters) throws Exception {
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
            public void open(Configuration parameters) throws Exception {
                simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            }

            @Override
            public OrderDetail map(String value) throws Exception {
                OrderDetail orderDetail = JSON.parseObject(value, OrderDetail.class);
                orderDetail.setCreate_ts(simpleDateFormat.parse(orderDetail.getCreate_time()).getTime());
                return orderDetail;
            }
        });



        env.execute();
    }
}