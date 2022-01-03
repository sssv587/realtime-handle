package com.futurebytedance.realtime.app.dwd;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.futurebytedance.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2021/12/20 - 23:58
 * @Description 准备用户行为日志DWD层
 */
public class BaseLogApp {
    private static final String TOPIC_START = "dwd_start_log";
    private static final String TOPIC_DISPLAY = "dwd_display_log";
    private static final String TOPIC_PAGE = "dwd_page_log";

    public static void main(String[] args) throws Exception {
        //TODO 1.准备环境
        //1.1 创建Flink流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //1.3 设置checkpoint
        //每5000ms开始一次checkpoint，模式是EXACTLY_ONCE(默认)
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setAlignmentTimeout(600000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/mall/checkpoint/baseLogApp"));

        //TODO 2.从kafka中读取数据
        //2.1 调用kafka工具类，获取FlinkKafkaConsumer
        String topic = "ods_base_log";
        String groupId = "base_log_app_group";
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> kafkaDataStream = env.addSource(kafkaSource);

        // 解决用户访问hdfs的问题
        System.setProperty("HADOOP_USER_NAME", "root");

        //TODO 3.对读取到的数据格式进行转换 String->json
        SingleOutputStreamOperator<JSONObject> jsonObjectDataStream = kafkaDataStream.map((MapFunction<String, JSONObject>) JSON::parseObject);

        //jsonObjDS.print("json>>>>>>");

        /* TODO 4.识别新老访客 前端也会对新老状态进行记录，有可能会不准，咱们这里是再次做一个确认
            保存mid某天访问情况(将首次访问日期作为状态保存起来)，等后面该设备在有日志过来的时候，从状态中获取日期
            和日志产生日期进行对比。如果状态不为空，并且状态日期和当前日志日期不相等，说明是老访客，如果is_new标记是1，那么对其状态进行修复
         */

        // 4.1 根据mid对日志进行分组
        KeyedStream<JSONObject, String> midKeyedDataStream = jsonObjectDataStream.keyBy((KeySelector<JSONObject, String>) data -> data.getJSONObject("common").get("mid").toString());
        //4.2 新老方法修复 状态分为算子状态和键控状态，我们这里要记录某一个设备的访问，使用键控状态比较合适
        SingleOutputStreamOperator<JSONObject> jsonDataStreamWithFlag = midKeyedDataStream.map(
                new RichMapFunction<JSONObject, JSONObject>() {
                    // 定义该mid的访问状态
                    private ValueState<String> firstVisitDataState;
                    // 定义日期格式化对象
                    private SimpleDateFormat sdf;

                    @Override
                    public void open(Configuration parameters) {
                        //对状态以及日期格式进行初始化
                        firstVisitDataState = getRuntimeContext().getState(new ValueStateDescriptor<String>("newMidDateState", String.class));
                        sdf = new SimpleDateFormat("yyyyMMdd");
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObj) throws Exception {
                        //获取当前日志标记状态
                        String isNew = jsonObj.getJSONObject("common").getString("is_new");

                        // 获取当前日志时间戳
                        Long ts = jsonObj.getLong("ts");

                        if ("1".equals(isNew)) {
                            //获取当前mid对象的状态
                            String stateDate = firstVisitDataState.value();
                            //对当前日志的日期格式进行转换
                            String curDate = sdf.format(new Date(ts));
                            //如果状态不为空，并且状态日期和当前日志日期不相等，说明是老访客，如果is_new标记是1，那么对其状态进行修复
                            if (stateDate != null && stateDate.length() != 0) {
                                // 判断是否为同一天数据
                                if (stateDate.equals(curDate)) {
                                    isNew = "0";
                                    jsonObj.getJSONObject("common").put("is_new", isNew);
                                }
                            } else {
                                //如果还没记录设备的状态，将当前访问日期作为访问状态值
                                firstVisitDataState.update(curDate);
                            }
                        }
                        return jsonObj;
                    }
                }
        );

        //TODO 5.分流 根据日志数据内容，将日志数据分为3类，页面日志、启动日志和曝光日志
        // 页面日志输出到主流，启动日志输出到启动侧输出流，曝光日志输出到曝光侧输出流
        // 侧输出流：1)接收迟到数据 2)分流

        // 定义启动侧输出流标签
        // 匿名内部类的子对象，需要获取类型，由于泛型擦除，所以需要加{},{}调用的是getTypeInfo()
        // new OutputTag<String>("start") -> Caused by: org.apache.flink.api.common.functions.InvalidTypesException: Could not determine TypeInformation for the OutputTag type. The most common reason is forgetting to make the OutputTag an anonymous inner class. It is also not possible to use generic type variables with OutputTags, such as ‘Tuple2<A, B>’.
        // 内部调用时做了泛型擦除
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        // 定义曝光侧输出流标签
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };

        SingleOutputStreamOperator<String> pageDataStream = jsonDataStreamWithFlag.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject jsonObject, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {
                // 获取启动日志标记
                JSONObject startJsonObj = jsonObject.getJSONObject("start");
                // 将json格式转换为字符串，方便向侧输出流输出以及向kafka中写入
                String dataStr = startJsonObj.toString();

                // 判断是否为启动日志
                if (startJsonObj.size() != 0) {
                    // 如果是启动日志，输出到启动侧输出流
                    ctx.output(startTag, dataStr);
                } else {
                    // 如果不是启动日志，获取曝光日志标记
                    JSONArray displays = jsonObject.getJSONArray("displays");
                    // 判断是否为曝光日志
                    if (displays != null && displays.size() != 0) {
                        // 如果是曝光日志，输出到侧输出流
                        for (int i = 0; i < displays.size(); i++) {
                            // 获取每一条曝光事件
                            JSONObject displayJsonObj = displays.getJSONObject(i);
                            // 获取页面id
                            String pageId = jsonObject.getJSONObject("page").getString("page_id");
                            // 给每一条曝光事件加pageIs
                            displayJsonObj.put("page_id", pageId);
                            ctx.output(displayTag, displayJsonObj.toString());
                        }
                    } else {
                        // 如果不是曝光日志，说明是页面日志，输出到主流
                        out.collect(dataStr);
                    }
                }
            }
        });

        // 获取侧输出流
        DataStream<String> startDataStream = pageDataStream.getSideOutput(startTag);
        DataStream<String> displayDataStream = pageDataStream.getSideOutput(displayTag);

        // 打印输出
        pageDataStream.print("page>>>>>");
        startDataStream.print("start>>>>>");
        displayDataStream.print("display>>>>>");

        //TODO 6.将不同流的数据写回到kafka的不同topic中
        FlinkKafkaProducer<String> startSink = MyKafkaUtil.getKafkaSink(TOPIC_START);
        startDataStream.addSink(startSink);

        FlinkKafkaProducer<String> displaySink = MyKafkaUtil.getKafkaSink(TOPIC_DISPLAY);
        displayDataStream.addSink(displaySink);

        FlinkKafkaProducer<String> pageSink = MyKafkaUtil.getKafkaSink(TOPIC_PAGE);
        pageDataStream.addSink(pageSink);

        env.execute();
    }
}