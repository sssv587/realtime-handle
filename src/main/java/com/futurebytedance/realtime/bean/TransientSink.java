package com.futurebytedance.realtime.bean;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2022/1/26 - 23:23
 * @Description 用该注解标记的属性，不需要插入到ClickHouse
 */
@Target(FIELD)
@Retention(RUNTIME)
public @interface TransientSink {
}
