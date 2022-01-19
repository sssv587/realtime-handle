package com.futurebytedance.realtime.utils;


import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2022/1/20 - 0:28
 * @Description 日期转换工具类
 * SimpleDateFormat存在线程安全问题，底层调用 calendar.setTime(date);
 * 解决：在JDK8之后，提供了DateTimeFormatter替代SimpleDateFormat
 */
public class DateTimeUtil {
    public static final DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) {
        System.out.println(ZoneId.systemDefault());
    }

    /**
     * 将Date日期转换为字符串
     *
     * @param date 日期
     * @return yyyy-MM-dd HH:mm:ss
     */
    public static String toyMdHms(Date date) {
        LocalDateTime localDateTime = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        return dtf.format(localDateTime);
    }

    public static Long toTimeStamp(String dataStr) {
        LocalDateTime localDateTime = LocalDateTime.parse(dataStr, dtf);
        return localDateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();
    }
}
