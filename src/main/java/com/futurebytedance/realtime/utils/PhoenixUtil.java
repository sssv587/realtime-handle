package com.futurebytedance.realtime.utils;

import com.futurebytedance.realtime.common.RealTimeConfig;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2022/1/16 - 20:54
 * @Description Phoenix工具类
 */
public class PhoenixUtil {
    private static Connection conn = null;

    public static void init() {
        try {
            //注册驱动
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            //获取Phoenix的连接
            conn = DriverManager.getConnection(RealTimeConfig.PHOENIX_SERVER);
            conn.setSchema(RealTimeConfig.HBASE_SCHEMA);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 从Phoenix中查询数据
     * select * from 表空间,表where XXX=xxx
     */
    public static <T> List<T> queryList(String sql, Class<T> clazz) {
        if (conn == null) {
            init();
        }
        List<T> resultList = new ArrayList<>();
        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            //获取数据库操作对象
            ps = conn.prepareStatement(sql);
            //执行SQL语句
            rs = ps.executeQuery();
            //通过结果集对象获取元数据信息
            ResultSetMetaData metaData = rs.getMetaData();
            //处理结果集
            while (rs.next()) {
                //声明一个对象，用于封装查询的一条结果集
                T rowData = clazz.newInstance();
                for (int i = 0; i < metaData.getColumnCount(); i++) {
                    BeanUtils.setProperty(rowData, metaData.getColumnName(i), rs.getObject(i));
                }
                resultList.add(rowData);
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("数据查询失败!");
        } finally {
            // 释放资源
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
        return resultList;
    }
}
