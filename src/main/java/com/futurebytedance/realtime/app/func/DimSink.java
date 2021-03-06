package com.futurebytedance.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.futurebytedance.realtime.common.RealTimeConfig;
import com.futurebytedance.realtime.utils.DimUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Set;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2022/1/6 - 23:47
 * @Description 写出维度数据的sink实现类
 */
public class DimSink extends RichSinkFunction<JSONObject> {
    /**
     * 定义Phoenix连接对象
     */
    private Connection conn = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        // 对连接对象进行初始化
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        conn = DriverManager.getConnection(RealTimeConfig.PHOENIX_SERVER);
    }

    @Override
    public void invoke(JSONObject jsonObj, Context context) {
        // 获取目标表的名称
        String tableName = jsonObj.getString("sink_table");
        // 获取json中data数据  data数据就是经过过滤之后  保留的业务表中字段
        JSONObject dataJsonObj = jsonObj.getJSONObject("data");

        if (dataJsonObj != null && dataJsonObj.size() > 0) {
            // 根据data中属性名和属性值  生成upsert语句
            String upsertSql = genUpsertSql(tableName.toUpperCase(), dataJsonObj);
            System.out.println("向Phoenix插入数据的SQL" + upsertSql);

            // 执行SQL
            try (PreparedStatement ps = conn.prepareStatement(upsertSql)) {
                ps.execute();
                // 注意：执行完Phoenix插入操作之后，需要手动提交事务
                conn.commit();
            } catch (SQLException e) {
                throw new RuntimeException("向Phoenix插入数据失败");
            }
            //如果当前做的是更新操作，需要将redis中缓存的数据给清除掉
            if ("update".equals(jsonObj.getString("type"))) {
                DimUtil.deleteCache(tableName, dataJsonObj.getString("id"));
            }
        }

    }

    // 根据data属性和值 生成向Phoenix中插入数据的sql语句

    private String genUpsertSql(String tableName, JSONObject dataJsonObj) {
        // "upsert into 表空间.表名(列名....) values(值....)
        Set<String> keys = dataJsonObj.keySet();
        Collection<Object> values = dataJsonObj.values();
        String upsertSql = "upsert into " + RealTimeConfig.HBASE_SCHEMA + "." + tableName
                + "(" + StringUtils.join(keys, ",") + ")";
        String valueSql = " values ('" + StringUtils.join(values, "','") + "')";
        return upsertSql + valueSql;
    }
}
