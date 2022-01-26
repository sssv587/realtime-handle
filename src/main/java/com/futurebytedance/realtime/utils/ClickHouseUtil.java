package com.futurebytedance.realtime.utils;

import com.futurebytedance.realtime.bean.TransientSink;
import com.futurebytedance.realtime.common.RealTimeConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.lang.reflect.Field;
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
                //执行写入操作 就是将当前流找那个的对象属性赋值给SQL占位符 insert into visitors_stats values(?,?,?,?,?,?,?,?,?,?,?,?)
                new JdbcStatementBuilder<T>() {
                    //obj 就是流中的一条数据对象
                    @Override
                    public void accept(PreparedStatement preparedStatement, T t) throws SQLException {
                        //TODO 具体的操作
                        //获取当前类中 所有属性
                        Field[] fields = t.getClass().getDeclaredFields();
                        //跳过的属性计数
                        int skipOffset = 0;
                        for (int i = 0; i < fields.length; i++) {
                            Field field = fields[i];
                            //通过属性对象获取属性上是否有@TransientSink注解
                            TransientSink transientSink = field.getAnnotation(TransientSink.class);
                            //如果transientSink不为空，说明属性上有@TransientSink标记，那么在给?赋值的时候，应该跳过当前属性
                            if (transientSink != null) {
                                skipOffset++;
                                continue;
                            }
                            //设置私有属性可访问
                            field.setAccessible(true);
                            //获取属性值对象
                            try {
                                Object o = field.get(t);
                                preparedStatement.setObject(i + 1 - skipOffset, o);
                            } catch (IllegalAccessException e) {
                                e.printStackTrace();
                            }
                        }
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
