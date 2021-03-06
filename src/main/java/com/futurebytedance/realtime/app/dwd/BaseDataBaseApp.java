package com.futurebytedance.realtime.app.dwd;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.futurebytedance.realtime.app.func.DimSink;
import com.futurebytedance.realtime.app.func.TableProcessFunction;
import com.futurebytedance.realtime.bean.TableProcess;
import com.futurebytedance.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;


/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2021/12/26 - 23:17
 * @Description 准备业务数据的dwd层
 */
public class BaseDataBaseApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.准备环境
        //1.1 创建流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //1.3 开启checkpoint，并设置相关的参数
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        env.setStateBackend(new FsStateBackend("hdfs://hadoop01:8020/mall/checkpoint/baseApp"));
        System.setProperty("HADOOP_USER_NAME", "root");
        // 重启策略
        // 如果没有开启重启checkpoint，那么重启策略就是noRestart
        // 如果开启了checkpoint，那么重启策略会自动帮你进行重启 重启次数Integer.MaxValue
        // env.setRestartStrategy(RestartStrategies.noRestart());

        //TODO 2.从kafka的ODS层读取数据
        String topic = "ods_base_db_m";
        String groupId = "base_db_app_group";

        //2.1 通过工具类
        FlinkKafkaConsumer<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> jsonStrDataStream = env.addSource(kafkaSource);

        //TODO 3.对DS中数据进行结构的转换  String -> Json
        SingleOutputStreamOperator<JSONObject> jsonObjectDataStream = jsonStrDataStream.map(JSON::parseObject);

        //TODO 4.对数据进行ETL清洗 如果table为空 或者data为空，或者长度<3，将这样的数据过滤掉
        SingleOutputStreamOperator<JSONObject> filterDataStream = jsonObjectDataStream.filter((FilterFunction<JSONObject>) value -> value.getString("table") != null && value.getJSONObject("data") != null
                && value.getString("data").length() >= 3);

        //TODO 5.动态分流 事实表放到主流，输出到kafka的DWD层；如果是维表，通过侧输出流，写入到Hbase
        //5.1 定义输出到Hbase的侧输出流标签
        OutputTag<JSONObject> hbaseTag = new OutputTag<JSONObject>(TableProcess.SINK_TYPE_HBASE) {
        };
        //5.2 主流
        SingleOutputStreamOperator<JSONObject> kafkaDataStream = filterDataStream.process(
                new TableProcessFunction(hbaseTag)
        );
        // 5.3 获取侧输出流 写到Hbase的数据
        DataStream<JSONObject> hbaseDataStream = kafkaDataStream.getSideOutput(hbaseTag);

        //TODO 6.将维度数据保存到phoenix对应的维度表中
        hbaseDataStream.addSink(new DimSink());

        //TODO 7.将事实数据写回到kafka的dwd层
        FlinkKafkaProducer<JSONObject> kafkaSink = MyKafkaUtil.getKafkaSinkBySchema(new KafkaSerializationSchema<JSONObject>() {
            @Override
            public void open(SerializationSchema.InitializationContext context) throws Exception {
                System.out.println("kafka序列化");
            }

            @Override
            public ProducerRecord<byte[], byte[]> serialize(JSONObject element, @Nullable Long timestamp) {
                String sinkTopic = element.getString("sink_table");
                JSONObject dataJsonObj = element.getJSONObject("data");
                return new ProducerRecord<>(sinkTopic, dataJsonObj.toString().getBytes());
            }
        });

        kafkaDataStream.addSink(kafkaSink);

        env.execute();
    }
}
