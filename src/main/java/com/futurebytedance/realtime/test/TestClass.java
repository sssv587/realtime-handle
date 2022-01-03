package com.futurebytedance.realtime.test;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.OutputTag;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2021/12/23 - 0:15
 * @Description
 */
public class TestClass {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        OutputTag<String> sss = new OutputTag<String>("sss"){};

        executionEnvironment.execute();
    }
}
