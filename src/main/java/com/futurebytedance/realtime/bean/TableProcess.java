package com.futurebytedance.realtime.bean;

import lombok.Data;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2021/12/27 - 0:30
 * @Description 配置表对应的实体类
 */
@Data
public class TableProcess {
    //动态分流 Sink 常量 改为小写和脚本一致
    public static final String SINK_TYPE_HBASE = "hbase";
    public static final String SINK_TYPE_KAFKA = "kafka";
    public static final String SINK_TYPE_CK = "clickhouse";

    //来源表
    String sourceTable;
    //操作类型 insert,update,delete
    String operateType;
    //输出类型 hbase kafka
    String sinkType;
    //输出表(主题)
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;
}
