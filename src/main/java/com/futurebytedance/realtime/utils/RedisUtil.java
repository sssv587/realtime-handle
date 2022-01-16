package com.futurebytedance.realtime.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2022/1/16 - 21:41
 * @Description 通过JedisPool连接池, 获取Jedis连接
 */
public class RedisUtil {
    private static JedisPool jedisPool = null;

    public static Jedis getJedis() {
        if (jedisPool == null) {
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            //最大可用连接数
            jedisPoolConfig.setMaxTotal(100);
            //连接耗尽是否等待
            jedisPoolConfig.setBlockWhenExhausted(true);
            //等待时间
            jedisPoolConfig.setMaxWaitMillis(2000);
            //最大闲置连接数
            jedisPoolConfig.setMaxIdle(5);
            //最小闲置连接数
            jedisPoolConfig.setMinIdle(5);
            //去连接的时候进行一下测试 ping pong
            jedisPoolConfig.setTestOnBorrow(true);
            jedisPool = new JedisPool(jedisPoolConfig, "hadoop01", 6379, 10000);
        }
        return jedisPool.getResource();
    }
}
