package com.futurebytedance.realtime.common;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2021/12/31 - 0:11
 * @Description 项目配置的常量类
 */
public class RealTimeConfig {
    /**
     * Hbase的命名空间
     */
    public static final String HBASE_SCHEMA = "HBASE_REALTIME";

    /**
     * Phoenix连接的服务器地址
     */
    public static final String PHOENIX_SERVER = "jdbc:phoenix:hadoop202,hadoop203,hadoop204:2181";

    /**
     * ClickHouse的URL链接地址
     */
    public static final String CLICKHOUSE_URL = "jdbc:clickhouse://hadoop01:8123/default";
}
