package com.futurebytedance.realtime.utils;

import com.futurebytedance.realtime.common.RealTimeConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2022/1/26 - 0:12
 * @Description 操作ClickHouse的工具类
 */
public class ClickHouseUtil {
    /**
     * 获取向ClickHouse中写入数据的SinkFUnction
     *
     * @param sql 执行的SQL
     * @param <T> 泛型
     * @return SinkFunction的实现类
     */
    public static <T> SinkFunction getJdbcSink(String sql) {
        return JdbcSink.<T>sink(
                //要执行的SQL语句
                sql,
                //执行写入操作
                new JdbcStatementBuilder<T>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {

                    }
                },
                //构建者设计模式，创建JdbcExecutionOptions对象，给batchSize属性赋值，执行批次大小
                new JdbcExecutionOptions.Builder().withBatchSize(5).build(),
                //构建者设计模式，创建JdbcConnectionOptions对象，给连接的相关属性进行赋值
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(RealTimeConfig.CLICKHOUSE_URL)
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .build()
        );
    }
}
