package com.futurebytedance.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2022/1/16 - 21:16
 * @Description 用于维度查询的工具类 底层调用的是PhoenixUtil
 * select * from dim_base_trademark where id = 10 and name = 'zs';
 */
public class DimUtil {
    /**
     * 从Phoenix中查询数据，没有使用缓存
     *
     * @param tableName       查询表名
     * @param cloNameAndValue 字段key、value映射
     * @return 查询结果集
     */
    @SafeVarargs
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>... cloNameAndValue) {
        //拼接查询条件
        StringBuilder whereSql = new StringBuilder(" where ");
        for (int i = 0; i < cloNameAndValue.length; i++) {
            Tuple2<String, String> tuple2 = cloNameAndValue[i];
            String fieldName = tuple2.f0;
            String fieldValue = tuple2.f1;
            if (i > 0) {
                whereSql.append("and ");
            }
            whereSql.append(fieldName).append("='").append(fieldValue).append("'");
        }

        String sql = "select * from " + tableName + whereSql;
        System.out.println("查询维度的SQL:" + sql);
        List<JSONObject> dimList = PhoenixUtil.queryList(sql, JSONObject.class);
        JSONObject dimJsonObj = null;
        //对于维度查询来说，一般都是根据主键进行查询，不可能返回多条记录，只会有一条
        if (dimList.size() > 0) {
            dimJsonObj = dimList.get(0);
        } else {
            System.out.println("维度数据没有找到:" + sql);
        }
        return dimJsonObj;
    }

    /**
     * 在做维度关联的时候，大部分场景都是通过id进行关联，所以提供一个方法，只需要将id作为参数传进来即可
     *
     * @param tableName 表名称
     * @param id        字段值
     * @return 查询结果
     */
    public static JSONObject getDimInfoNoCache(String tableName, String id) {
        return getDimInfoNoCache(tableName, Tuple2.of("id", id));
    }

    /**
     * 优化：从Phoenix中查询数据，加入了旁路缓存
     * 先从缓存查询，如果缓存没有查询到数据，再到Phoenix查询，并将查询结果放到缓存中
     * <p>
     * redis
     * 类型:       string
     * Key:        dim:表名:值   dim:BASE_DIM_TRADEMARK:10_xxx
     * Value:      通过PhoenixUtil到维度表中查询数据，取出第一条并将其转换为json字符串
     * 失效时间:    24*3600
     * <p>
     * //"DIM_BASE_TRADEMARK",Tuple2.of("id","13"),Tuple2("tm_name","zz")
     * <p>
     * redisKey="dim:dim_base_trademark"
     * where id='13' and tm_name='zz'
     * <p>
     * dim:dim_base_trademark:13_zz ----> Json
     */
    @SafeVarargs
    public static JSONObject getDimInfoCache(String tableName, Tuple2<String, String>... cloNameAndValue) {
        //拼接查询条件
        StringBuilder whereSql = new StringBuilder(" where ");
        StringBuilder redisKey = new StringBuilder("dim:" + tableName.toLowerCase() + ":");
        for (int i = 0; i < cloNameAndValue.length; i++) {
            Tuple2<String, String> tuple2 = cloNameAndValue[i];
            String fieldName = tuple2.f0;
            String fieldValue = tuple2.f1;
            if (i > 0) {
                whereSql.append(" and ");
                redisKey.append("_");
            }
            whereSql.append(fieldName).append("='").append(fieldValue).append("'");
            redisKey.append(fieldValue);
        }

        //从Redis中获取数据
        Jedis jedis;
        //维度数据的json字符串形式
        String dimJsonStr;
        //
        JSONObject dimJsonObj = null;
        try {
            //获取Redis客户端
            jedis = RedisUtil.getJedis();
            //根据key到Redis中查询
            dimJsonStr = jedis.get(redisKey.toString());
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从redis中查询维度失败");
        }

        //判断是否从redis中查询到了数据
        if (dimJsonStr != null && dimJsonStr.length() > 0) {
            dimJsonObj = JSON.parseObject(dimJsonStr);
        } else {
            //如果在redis中没有查到数据，需要到Phoenix中查询
            String sql = "select * from " + tableName + whereSql;
            System.out.println("查询维度的SQL:" + sql);
            List<JSONObject> dimList = PhoenixUtil.queryList(sql, JSONObject.class);
            //对于维度查询来说，一般都是根据主键进行查询，不可能返回多条记录，只会有一条
            if (dimList.size() > 0) {
                dimJsonObj = dimList.get(0);
                //将查询出来的数据放到redis中缓存起来
                if (jedis != null) {
                    jedis.setex(redisKey.toString(), 24 * 3600, dimJsonObj.toJSONString());
                }
            } else {
                System.out.println("维度数据没有找到:" + sql);
            }
        }

        //关闭Jedis
        if (jedis != null) {
            jedis.close();
        }

        return dimJsonObj;
    }

    //根据key让缓存失效
    public static void deleteCache(String tableName, String primaryKey) {
        String key = "dim:" + tableName.toLowerCase() + ":" + primaryKey;
        try {
            Jedis jedis = RedisUtil.getJedis();
            //通过key清除缓存
            jedis.del(key);
            jedis.close();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("缓存异常！");
        }
    }
}
