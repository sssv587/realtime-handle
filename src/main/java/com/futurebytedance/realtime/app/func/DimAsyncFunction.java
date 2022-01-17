package com.futurebytedance.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.futurebytedance.realtime.utils.DimUtil;
import com.futurebytedance.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.util.Collections;
import java.util.concurrent.ExecutorService;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2022/1/17 - 23:30
 * @Description 自定义维度异步查询的函数
 * 模板方法设计模式
 *      在父类中只定义方法的声明，让整个流程跑通
 *      具体地实现延迟到子类中实现
 */
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T, T> implements DimJoinFunction<T> {
    /**
     * 线程池对象的父接口声明(多态)
     */
    private ExecutorService executorService;

    /**
     * 维度的表名
     */
    private final String tableName;

    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    @Override
    public void open(Configuration parameters) {
        //初始化线程池对象
        System.out.println("初始化线程池对象");
        executorService = ThreadPoolUtil.getInstance();
    }

    /**
     * 发送异步请求的方法
     *
     * @param input        流中的事实数据
     * @param resultFuture 异步处理结束之后返回结果
     */
    @Override
    public void asyncInvoke(T input, ResultFuture<T> resultFuture) {
        try {
            executorService.submit(() -> {
                //发送异步请求
                long start = System.currentTimeMillis();
                //从事实数据流中获取key
                String key = getKey(input);

                //根据维度的主键到维度表中进行查询
                JSONObject dimInfoJsonObj = DimUtil.getDimInfoNoCache(tableName, key);
                System.out.println("维度数据Json格式:" + dimInfoJsonObj);

                if (dimInfoJsonObj != null) {
                    //维度关联 流中的事实数据和查询出来的维度数据进行关联
                    join(input, dimInfoJsonObj);
                }
                System.out.println("维度关联后的对象:" + input);

                long end = System.currentTimeMillis();
                System.out.println("异步维度查询耗时" + (end - start) + "毫秒");
                //将关联后的数据继续向下传递
                resultFuture.complete(Collections.singletonList(input));
            });
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(tableName + "维度异步查询失败");
        }
    }
}
