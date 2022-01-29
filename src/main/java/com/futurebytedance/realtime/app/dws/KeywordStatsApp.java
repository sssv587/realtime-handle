package com.futurebytedance.realtime.app.dws;

import com.futurebytedance.realtime.app.func.KeywordUDTF;
import com.futurebytedance.realtime.bean.KeywordStats;
import com.futurebytedance.realtime.common.MallConstant;
import com.futurebytedance.realtime.utils.ClickHouseUtil;
import com.futurebytedance.realtime.utils.MyKafkaUtil;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2022/1/29 - 16:20
 * @Description DWS层 搜索关键字
 */
public class KeywordStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        //1.1 创建Flink流式处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 设置并行度
        env.setParallelism(4);
        //1.3 检查点 CK 相关设置
        env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);
        StateBackend fsStateBackend = new FsStateBackend(
                "hdfs://hadoop01:8020/mall/flink/checkpoint/KeywordStatsApp");
        env.setStateBackend(fsStateBackend);
        System.setProperty("HADOOP_USER_NAME", "root");
        //1.4 创建Table环境
        EnvironmentSettings setting = EnvironmentSettings
                .newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, setting);

        //TODO 2.注册自定义函数
        tableEnv.createTemporaryFunction("ik_analyze", KeywordUDTF.class);

        //TODO 3.创建动态表
        //3.1 声明主题以及消费者组
        String groupId = "keyword_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        //3.2 建表
        tableEnv.executeSql("CREATE TABLE page_view " +
                "(common MAP<STRING,STRING>, " +
                "page MAP<STRING,STRING>,ts BIGINT, " +
                "rowtime AS TO_TIMESTAMP(FROM_UNIXTIME(ts/1000, 'yyyy-MM-dd HH:mm:ss')) ," +
                "WATERMARK FOR rowtime AS rowtime - INTERVAL '2' SECOND) " +
                "WITH (" + MyKafkaUtil.getKafkaDDL(pageViewSourceTopic, groupId) + ")");

        //TODO 4.从动态表中查询数据
        Table fullWordView = tableEnv.sqlQuery("select page['item'] fullword ," +
                "rowtime from page_view " +
                "where page['page_id']='good_list' " +
                "and page['item'] IS NOT NULL ");

        //TODO 5.利用自定义函数 对搜索关键词进行拆分
        Table keywordTable = tableEnv.sqlQuery("select keyword,rowtime from " + fullWordView + " ," +
                " LATERAL TABLE(ik_analyze(fullword)) as T(keyword)");

        //TODO 6.聚合
        Table reduceTable = tableEnv.sqlQuery("select keyword,count(*) ct, '"
                + MallConstant.KEYWORD_SEARCH + "' source ," +
                "DATE_FORMAT(TUMBLE_START(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') stt," +
                "DATE_FORMAT(TUMBLE_END(rowtime, INTERVAL '10' SECOND),'yyyy-MM-dd HH:mm:ss') edt," +
                "UNIX_TIMESTAMP()*1000 ts from " + keywordTable
                + " GROUP BY TUMBLE(rowtime, INTERVAL '10' SECOND ),keyword");

        //TODO 7.转换为流
        DataStream<KeywordStats> keywordStatsDataStream = tableEnv.toAppendStream(reduceTable, KeywordStats.class);

        //TODO 8.写入到ClickHouse
        keywordStatsDataStream.addSink(
                ClickHouseUtil.getJdbcSink("insert into keyword_stats(keyword,ct,source,stt,edt,ts) " +
                        " values(?,?,?,?,?,?)")
        );

        env.execute();
    }
}
