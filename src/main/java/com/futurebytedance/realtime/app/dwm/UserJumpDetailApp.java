package com.futurebytedance.realtime.app.dwm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.futurebytedance.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2022/1/11 - 23:24
 * @Description 用户跳出行为过滤
 */
public class UserJumpDetailApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 准备本地测试流环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //1.3 设置Checkpoint
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://localhost:8020/mall/checkpoint/unique"));

        //TODO 2.从kafka中读取数据
        String sourceTopic = "dwd_page_log";
        String groupId = "user_jump_detail_group";
        String sinkTopic = "dwm_user_dump_detail";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(sourceTopic, groupId);
        DataStreamSource<String> jsonStrDataStream = env.addSource(kafkaSource);

        //TODO 3.对读取到的数据进行结构的转换
        SingleOutputStreamOperator<JSONObject> jsonObjDataStream = jsonStrDataStream.map(JSON::parseObject);

        // 注意：从Flink1.12版本开始，默认的时间语义就是事件时间，不需要额外指定；如果是之前的版本，需要通过如下语句指定事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // TODO 4.指定事件时间字段
        SingleOutputStreamOperator<JSONObject> jsonObjWithTS = jsonObjDataStream.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                    @Override
                    public long extractTimestamp(JSONObject element, long recordTimestamp) {
                        return element.getLong("ts");
                    }
                }));

        //TODO 5.按照mid进行分组
        jsonObjWithTS.keyBy(jsonStr -> jsonStr.getJSONObject("common").getString("mid"));

        /*
         * 计算页面跳出明细，需要满足两个条件
         *      1.不是从其他页面跳转过来的页面，是一个首次访问页面
         *          last_page_id=null
         *      2.距离首次访问结束后10秒内，没有对其他的页面再进行访问
         */
        //TODO 6.配置CEP表达式
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("first")
                .where(
                        //条件1 不是从其他页面跳转过来的页面，是一个首次访问页面
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject value) {
                                //获取last_page_id
                                String lastPageId = value.getJSONObject("page").getString("last_page_id");
                                //判断是否为null 将为空的保留，非空的过滤掉
                                return lastPageId == null || lastPageId.length() == 0;
                            }
                        }
                ).next("next")
                .where(
                        //条件2 距离首次访问结束后10秒内，没有对其他的页面再进行访问
                        new SimpleCondition<JSONObject>() {
                            @Override
                            public boolean filter(JSONObject value) {
                                //获取当前页面的id
                                String pageId = value.getJSONObject("page").getString("page_id");
                                //判断当前访问的页面id是否为null
                                return pageId != null && pageId.length() > 0;
                            }
                        })
                .within(Time.milliseconds(10000));


        env.execute();
    }
}
