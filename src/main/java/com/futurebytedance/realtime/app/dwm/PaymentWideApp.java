package com.futurebytedance.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.futurebytedance.realtime.bean.OrderWide;
import com.futurebytedance.realtime.bean.PaymentInfo;
import com.futurebytedance.realtime.bean.PaymentWide;
import com.futurebytedance.realtime.utils.DateTimeUtil;
import com.futurebytedance.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2022/1/19 - 23:51
 * @Description 支付款表处理程序
 */
public class PaymentWideApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境的准备
        //1.1 创建流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //1.3 检查点相关的配置
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/mall/"));

        //TODO 2.从kafka的主题中读取数据
        //2.1 声明相关的主题以及消费者组
        String paymentInfoSourceTopic = "dwd_payment_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSinkTopic = "dwm_payment_wide";
        String groupId = "payment_wide_app_group";

        //2.2 读取支付数据
        FlinkKafkaConsumer<String> paymentInfoSource = MyKafkaUtil.getKafkaSource(paymentInfoSourceTopic, groupId);
        DataStreamSource<String> paymentInfoJsonStrDS = env.addSource(paymentInfoSource);

        //2.3 读取订单宽表数据
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        DataStreamSource<String> orderWideJsonStrDS = env.addSource(orderWideSource);

        //TODO 3.对读取到的数据进行结构的转换 jsonStr->POJO
        //3.1 转换支付流
        SingleOutputStreamOperator<PaymentInfo> paymentInfoDS = paymentInfoJsonStrDS.map(jsonStr -> JSON.parseObject(jsonStr, PaymentInfo.class));
        //3.2 转换订单流
        SingleOutputStreamOperator<OrderWide> orderWideDS = orderWideJsonStrDS.map(jsonStr -> JSON.parseObject(jsonStr, OrderWide.class));

        //TODO 4.设置Watermark以及事件时间字段
        //4.1 支付流的Watermark
        SingleOutputStreamOperator<PaymentInfo> paymentWithWM = paymentInfoDS.assignTimestampsAndWatermarks(WatermarkStrategy.<PaymentInfo>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<PaymentInfo>() {
                    @Override
                    public long extractTimestamp(PaymentInfo element, long recordTimestamp) {
                        //需要将字符串的时间转换为毫秒数
                        return DateTimeUtil.toTimeStamp(element.getCallback_time());
                    }
                }));

        //4.2 订单流的Watermark
        SingleOutputStreamOperator<OrderWide> orderWideWithWM = orderWideDS.assignTimestampsAndWatermarks(WatermarkStrategy.<OrderWide>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                .withTimestampAssigner(new SerializableTimestampAssigner<OrderWide>() {
                    @Override
                    public long extractTimestamp(OrderWide orderWide, long recordTimestamp) {
                        return DateTimeUtil.toTimeStamp(orderWide.getCreate_time());
                    }
                })
        );

        //TODO 5.对数据进行分组
        //5.1 支付流数据分组
        KeyedStream<PaymentInfo, Long> paymentInfoKeyedDS = paymentWithWM.keyBy(PaymentInfo::getOrder_id);
        //5.2 订单宽表流数据分组
        KeyedStream<OrderWide, Long> orderWideKeyedDS = orderWideWithWM.keyBy(OrderWide::getOrder_id);

        //TODO 6.使用IntervalJoin关联两条流
        SingleOutputStreamOperator<PaymentWide> paymentWideDS = paymentInfoKeyedDS.intervalJoin(orderWideKeyedDS).between(Time.seconds(-1800), Time.seconds(0))
                .process(new ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>() {
                    @Override
                    public void processElement(PaymentInfo paymentInfo, OrderWide orderWide, ProcessJoinFunction<PaymentInfo, OrderWide, PaymentWide>.Context ctx, Collector<PaymentWide> out) {
                        out.collect(new PaymentWide(paymentInfo, orderWide));
                    }
                });

        //TODO 7.将数据写入kafka的DWM层
        paymentWideDS.map(JSON::toJSONString).addSink(MyKafkaUtil.getKafkaSink(paymentWideSinkTopic));

        env.execute();
    }
}
