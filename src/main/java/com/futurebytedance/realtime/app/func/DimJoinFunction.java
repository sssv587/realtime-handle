package com.futurebytedance.realtime.app.func;

import com.alibaba.fastjson.JSONObject;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2022/1/18 - 0:08
 * @Description 维度关联接口
 */
public interface DimJoinFunction<T> {
    /**
     * 需要提供一个获取key的方法，但是这个方法如何实现不知道
     *
     * @param obj 传入的对象
     * @return 返回需要查询的key
     */
    String getKey(T obj);

    /**
     * 两个对象之间关联
     *
     * @param obj         传入的对象
     * @param dimInfoJson 传入的JsonObject对象
     */
    void join(T obj, JSONObject dimInfoJson);
}
