package com.futurebytedance.realtime.utils;

import com.futurebytedance.realtime.bean.TableProcess;
import com.google.common.base.CaseFormat;
import org.apache.commons.beanutils.BeanUtils;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2021/12/27 - 0:34
 * @Description 从mysql数据中心查询数据的工具类
 * 完成ORM，对象关系映射
 * O:Object对象   Java中对象
 * R:Relation关系 关系型数据库
 * M:Mapping映射  将Java中的对象和关系型数据库的表中的记录建立起映射关系
 * 数据库                           类
 * 表:t_student                       Student
 * 字段:id,name                      属性:id,name
 * 记录:100,zs                        对象:100,zs
 * ResultSet(一条条记录)             List(一个个java对象)
 */
public class MysqlUtil {
    /**
     * @param sql                要执行的查询语句
     * @param clz                返回的数据类型
     * @param underSourceToCamel 是否将下划线转换为驼峰命名法
     * @param <T>                泛型
     * @return 返回的封装查询结果
     */
    public static <T> List<T> queryList(String sql, Class<T> clz, boolean underSourceToCamel) {
        Connection conn = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
            // 注册驱动
            Class.forName("com.mysql.jdbc.Driver");
            // 创建连接
            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/realtime?characterEncoding=utf-8&useSSL=false", "root", "123456");
            // 创建数据库操作对象
            ps = conn.prepareStatement(sql);
            // 执行sql语句
            rs = ps.executeQuery();
            // 查询结果的元数据信息
            ResultSetMetaData metaData = rs.getMetaData();
            List<T> resultList = new ArrayList<>();
            // 判断结果集中是否存在数据，如果有，那么进行一次循环
            while (rs.next()) {
                // 创建一个对象，用于封装查询出来一条结果集中的数据
                T obj = clz.newInstance();
                // 对查询的所有列进行遍历，获取每一列的名称
                for (int i = 1; i <= metaData.getColumnCount(); i++) {
                    String propertyName = metaData.getColumnName(i);
                    if (underSourceToCamel) {
                        // 如果指定将下划线转换为驼峰命名法的值为 true，通过guava工具类，需要将表中的列转换为类属性的驼峰命名法形式
                        propertyName = CaseFormat.LOWER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, propertyName);
                    }
                    // 调用apache的commons-bean中工具类，给obj属性赋值
                    BeanUtils.setProperty(obj, propertyName, rs.getObject(i));
                }
                // 将当前结果集中的一行数据封装的obj对象放到list集合中
                resultList.add(obj);
            }
            // 处理结果集
            return resultList;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从Mysql查询结果失败");
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
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) {
        List<TableProcess> tableProcesses = queryList("select * from table_process",
                TableProcess.class, true);
        for (TableProcess tableProcess : tableProcesses) {
            System.out.println(tableProcess);
        }
    }
}
