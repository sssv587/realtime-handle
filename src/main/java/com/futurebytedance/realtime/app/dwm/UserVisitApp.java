package com.futurebytedance.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.futurebytedance.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2022/1/9 - 23:24
 * @Description 独立访客UV计算
 */
public class UserVisitApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 准备本地测试流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //1.2 设置并行度
        env.setParallelism(4);
        //1.3 设置Checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/mall/checkpoint/unique"));

        //TODO 2.从kafka中读取数据
        String sourceTopic = "dwd_page_log";
        String groupId = "unique_visit_app";
        String sinkTopic = "dwm_unique_visit";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> jsonStrDataStream = env.addSource(kafkaSource);

        //TODO 3.对读取到的数据进行结构的转换
        SingleOutputStreamOperator<JSONObject> jsonObjDataStream = jsonStrDataStream.map(JSON::parseObject);

        //TODO 4.按照设备ID进行分组
        KeyedStream<JSONObject, String> keyByMidDataStream = jsonObjDataStream.keyBy(jsonObj -> jsonObj.getJSONObject("common").getString("mid"));

        //TODO 5.过滤得到UV
        SingleOutputStreamOperator<JSONObject> filterDataStream = keyByMidDataStream.filter(new RichFilterFunction<JSONObject>() {
            //定义状态
            ValueState<String> lastVisitDataState = null;
            //定义日期工具类
            SimpleDateFormat sdf = null;

            @Override
            public void open(Configuration parameters) {
                //初始化状态以及日期工具类
                sdf = new SimpleDateFormat("yyyyMMdd");
                //初始化状态
                ValueStateDescriptor<String> lastVisitDataState = new ValueStateDescriptor<>("lastVisitDataState", String.class);
                //因为我们统计的是日活DAU，所以状态数据只在当天有效，过了一天就可以失效掉
                lastVisitDataState.enableTimeToLive(StateTtlConfig.newBuilder(Time.days(1)).build());
                this.lastVisitDataState = getRuntimeContext().getState(lastVisitDataState);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //首先判断当前页面是否从别的页面跳转过来的
                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                if (lastPageId != null && lastPageId.length() > 0) {
                    return false;
                }

                //获取当前访问时间
                Long ts = value.getLong("ts");
                //将当前访问时间转换为日期字符串
                String logDate = sdf.format(new Date(ts));
                //获取状态日期
                String lastVisitDate = lastVisitDataState.value();

                //用当前页面的访问时间和状态时间进行对比
                if (lastVisitDate != null && lastVisitDate.length() > 0 && lastVisitDate.equals(logDate)) {
                    System.out.println("已访问:lastVisitDate-" + lastVisitDate + ",||logDate:" + logDate);
                    return false;
                } else {
                    System.out.println("未访问:lastVisitDate-" + lastVisitDate + ",||logDate:" + logDate);
                    lastVisitDataState.update(logDate);
                    return true;
                }
            }
        });

        //向kafka中写回，需要将json转换为String
        //6.1 json->String
        SingleOutputStreamOperator<String> kafkaDataStream = filterDataStream.map(JSON::toString);

        //6.2 写回到kafka的dwm层
        kafkaDataStream.addSink(MyKafkaUtil.getKafkaSink(sinkTopic));

        env.execute();
    }
}
