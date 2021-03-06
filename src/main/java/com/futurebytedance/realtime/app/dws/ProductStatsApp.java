package com.futurebytedance.realtime.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.futurebytedance.realtime.app.func.DimAsyncFunction;
import com.futurebytedance.realtime.bean.OrderWide;
import com.futurebytedance.realtime.bean.PaymentWide;
import com.futurebytedance.realtime.bean.ProductStats;
import com.futurebytedance.realtime.common.MallConstant;
import com.futurebytedance.realtime.utils.ClickHouseUtil;
import com.futurebytedance.realtime.utils.DateTimeUtil;
import com.futurebytedance.realtime.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.TimeUnit;

/**
 * @author yuhang.sun
 * @version 1.0
 * @date 2022/1/26 - 23:53
 * @Description ????????????????????????
 * ??????????????????????????????
 * -zk,kafka,logger.sh(nginx + ??????????????????),maxwell,hdfs,hbase,Redis,ClickHouse
 * -BaseLogApp,BaseDBApp,OrderWideApp,PaymentWide,ProductStatsApp
 */
public class ProductStatsApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.??????????????????
        //1.1 ??????Flink??????????????????
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.2 ???????????????
        env.setParallelism(4);
         /*
         //1.3 ????????? CK ????????????
         env.enableCheckpointing(5000, CheckpointingMode.AT_LEAST_ONCE);
         env.getCheckpointConfig().setCheckpointTimeout(60000);
         StateBackend fsStateBackend = new FsStateBackend(
         "hdfs://hadoop01:8020/mall/flink/checkpoint/ProductStatsApp");
         env.setStateBackend(fsStateBackend);
         System.setProperty("HADOOP_USER_NAME","root");
         */

        //TODO 2.???Kafka??????????????????
        //2.1 ?????????????????????????????????????????????
        String groupId = "product_stats_app";
        String pageViewSourceTopic = "dwd_page_log";
        String favorInfoSourceTopic = "dwd_favor_info";
        String cartInfoSourceTopic = "dwd_cart_info";
        String orderWideSourceTopic = "dwm_order_wide";
        String paymentWideSourceTopic = "dwm_payment_wide";
        String refundInfoSourceTopic = "dwd_order_refund_info";
        String commentInfoSourceTopic = "dwd_comment_info";

        //2.2 ?????????????????????????????????????????????
        FlinkKafkaConsumer<String> pageViewSource = MyKafkaUtil.getKafkaSource(pageViewSourceTopic, groupId);
        DataStreamSource<String> pageViewDStream = env.addSource(pageViewSource);

        //2.3 ???dwd_favor_info?????????????????????
        FlinkKafkaConsumer<String> favorInfoSourceSource = MyKafkaUtil.getKafkaSource(favorInfoSourceTopic, groupId);
        DataStreamSource<String> favorInfoDStream = env.addSource(favorInfoSourceSource);

        //2.4 ???dwd_cart_info????????????????????????
        FlinkKafkaConsumer<String> cartInfoSource = MyKafkaUtil.getKafkaSource(cartInfoSourceTopic, groupId);
        DataStreamSource<String> cartInfoDStream = env.addSource(cartInfoSource);

        //2.5 ???dwm_order_wide?????????????????????
        FlinkKafkaConsumer<String> orderWideSource = MyKafkaUtil.getKafkaSource(orderWideSourceTopic, groupId);
        DataStreamSource<String> orderWideDStream = env.addSource(orderWideSource);

        //2.6 ???dwm_payment_wide?????????????????????
        FlinkKafkaConsumer<String> paymentWideSource = MyKafkaUtil.getKafkaSource(paymentWideSourceTopic, groupId);
        DataStreamSource<String> paymentWideDStream = env.addSource(paymentWideSource);

        //2.7 ???dwd_order_refund_info?????????????????????
        FlinkKafkaConsumer<String> refundInfoSource = MyKafkaUtil.getKafkaSource(refundInfoSourceTopic, groupId);
        DataStreamSource<String> refundInfoDStream = env.addSource(refundInfoSource);

        //2.8 ???dwd_order_refund_info?????????????????????
        FlinkKafkaConsumer<String> commentInfoSource = MyKafkaUtil.getKafkaSource(commentInfoSourceTopic, groupId);
        DataStreamSource<String> commentInfoDStream = env.addSource(commentInfoSource);

        //TODO 3.??????????????????????????????????????????????????? ProduceStats
        //3.1 ???????????????????????????????????? jsonStr->ProduceStats
        SingleOutputStreamOperator<ProductStats> productClickAndDisplayDS = pageViewDStream.process(
                new ProcessFunction<String, ProductStats>() {
                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, ProductStats>.Context ctx, Collector<ProductStats> out) throws Exception {
                        //???json????????????????????????json??????
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        JSONObject pageJsonObj = jsonObj.getJSONObject("page");
                        String pageId = pageJsonObj.getString("page_id");
                        if (pageId == null) {
                            System.out.println(">>>>" + jsonObj);
                        }
                        //??????????????????
                        Long ts = jsonObj.getLong("ts");
                        //?????????????????????????????????????????????????????????????????????????????????
                        if ("good_detail".equals(pageId)) {
                            //?????????????????????id
                            Long skuId = pageJsonObj.getLong("item");
                            //????????????????????????
                            ProductStats productStats = ProductStats.builder().sku_id(skuId).click_ct(1L).ts(ts).build();
                            //???????????????
                            out.collect(productStats);
                        }

                        JSONArray displays = jsonObj.getJSONArray("displays");
                        //??????display?????????????????????????????????????????????
                        if (displays != null && displays.size() > 0) {
                            for (int i = 0; i < displays.size(); i++) {
                                JSONObject displaysJSONObject = displays.getJSONObject(i);
                                //????????????????????????????????????
                                if ("sku_id".equals(displaysJSONObject.getString("item_type"))) {
                                    //????????????id
                                    Long skuId = displaysJSONObject.getLong("item");
                                    //????????????????????????
                                    ProductStats productStats = ProductStats.builder().sku_id(skuId).display_ct(1L).ts(ts).build();
                                    //???????????????
                                    out.collect(productStats);
                                }
                            }
                        }
                    }
                }
        );

        //3.2 ??????????????????????????? jsonStr-->ProductStats
        SingleOutputStreamOperator<ProductStats> orderWideStatsDS = orderWideDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String jsonStr) {
                //???json?????????????????????????????????????????????
                OrderWide orderWide = JSON.parseObject(jsonStr, OrderWide.class);
                String create_time = orderWide.getCreate_time();
                //????????????????????????????????????
                Long ts = DateTimeUtil.toTimeStamp(create_time);
                return ProductStats.builder().sku_id(orderWide.getSku_id())
                        .order_sku_num(orderWide.getSku_num())
                        .order_amount(orderWide.getSplit_total_amount())
                        .ts(ts)
                        .orderIdSet(new HashSet(Collections.singleton(orderWide.getOrder_id())))
                        .build();
            }
        });

        //3.3 ?????????????????????
        SingleOutputStreamOperator<ProductStats> favorStatsDS = favorInfoDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String jsonStr) {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                Long ts = DateTimeUtil.toTimeStamp(jsonObj.getString("create_time"));

                return ProductStats.builder()
                        .sku_id(jsonObj.getLong("sku_id"))
                        .favor_ct(1L)
                        .ts(ts)
                        .build();
            }
        });

        //3.4 ????????????????????????
        SingleOutputStreamOperator<ProductStats> cartStatsDS = cartInfoDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String jsonStr) {
                JSONObject jsonObj = JSON.parseObject(jsonStr);
                Long ts = DateTimeUtil.toTimeStamp(jsonObj.getString("create_time"));

                return ProductStats.builder()
                        .cart_ct(1L)
                        .ts(ts)
                        .build();
            }
        });

        //3.5 ?????????????????????
        SingleOutputStreamOperator<ProductStats> paymentStatsDS = paymentWideDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String json) {
                PaymentWide paymentWide = JSON.parseObject(json, PaymentWide.class);
                Long ts = DateTimeUtil.toTimeStamp(paymentWide.getPayment_create_time());
                return ProductStats.builder().sku_id(paymentWide.getSku_id())
                        .payment_amount(paymentWide.getSplit_total_amount())
                        .paidOrderIdSet(new HashSet(Collections.singleton(paymentWide.getOrder_id())))
                        .ts(ts).build();
            }
        });

        //3.6 ?????????????????????
        SingleOutputStreamOperator<ProductStats> refundStatsDS = refundInfoDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String json) {
                JSONObject refundJsonObj = JSON.parseObject(json);
                Long ts = DateTimeUtil.toTimeStamp(refundJsonObj.getString("create_time"));
                return ProductStats.builder()
                        .sku_id(refundJsonObj.getLong("sku_id"))
                        .refund_amount(refundJsonObj.getBigDecimal("refund_amount"))
                        .refundOrderIdSet(
                                new HashSet(Collections.singleton(refundJsonObj.getLong("order_id"))))
                        .ts(ts).build();
            }
        });

        //3.7 ?????????????????????
        SingleOutputStreamOperator<ProductStats> commonInfoStatsDS = commentInfoDStream.map(new MapFunction<String, ProductStats>() {
            @Override
            public ProductStats map(String json) {
                JSONObject commonJsonObj = JSON.parseObject(json);
                Long ts = DateTimeUtil.toTimeStamp(commonJsonObj.getString("create_time"));
                Long goodCt = MallConstant.APPRAISE_GOOD.equals(commonJsonObj.getString("appraise")) ?
                        1L : 0L;
                return ProductStats.builder()
                        .sku_id(commonJsonObj.getLong("sku_id"))
                        .comment_ct(1L)
                        .good_comment_ct(goodCt)
                        .ts(ts)
                        .build();
            }
        });

        //TODO 4.??????????????????????????????
        DataStream<ProductStats> unionDS = productClickAndDisplayDS.union(
                orderWideStatsDS,
                favorStatsDS,
                cartStatsDS,
                paymentStatsDS,
                refundStatsDS,
                commonInfoStatsDS
        );

        //TODO 5.??????Watermark??????????????????????????????
        SingleOutputStreamOperator<ProductStats> productStatsWithWatermarkDS = unionDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<ProductStats>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<ProductStats>() {
                                    @Override
                                    public long extractTimestamp(ProductStats productStats, long recordTimestamp) {
                                        return productStats.getTs();
                                    }
                                }
                        )
        );

        //TODO 6.?????????????????????????????????
        KeyedStream<ProductStats, Long> keyedDS = productStatsWithWatermarkDS.keyBy(
                new KeySelector<ProductStats, Long>() {
                    @Override
                    public Long getKey(ProductStats productStats) throws Exception {
                        return productStats.getSku_id();
                    }
                }
        );

        //TODO 7.???????????????????????????????????? ?????????10s???????????????
        WindowedStream<ProductStats, Long, TimeWindow> windowDS = keyedDS.window(
                TumblingEventTimeWindows.of(Time.seconds(10))
        );

        //TODO 8.?????????????????????????????????
        SingleOutputStreamOperator<ProductStats> reduceDS = windowDS.reduce(
                new ReduceFunction<ProductStats>() {
                    @Override
                    public ProductStats reduce(ProductStats stats1, ProductStats stats2) {
                        stats1.setDisplay_ct(stats1.getDisplay_ct() + stats2.getDisplay_ct());
                        stats1.setClick_ct(stats1.getClick_ct() + stats2.getClick_ct());
                        stats1.setCart_ct(stats1.getCart_ct() + stats2.getCart_ct());
                        stats1.setFavor_ct(stats1.getFavor_ct() + stats2.getFavor_ct());
                        stats1.setOrder_amount(stats1.getOrder_amount().add(stats2.getOrder_amount()));
                        stats1.getOrderIdSet().addAll(stats2.getOrderIdSet());
                        stats1.setOrder_ct(stats1.getOrderIdSet().size() + 0L);
                        stats1.setOrder_sku_num(stats1.getOrder_sku_num() + stats2.getOrder_sku_num());
                        stats1.setPayment_amount(stats1.getPayment_amount().add(stats2.getPayment_amount()));

                        stats1.getRefundOrderIdSet().addAll(stats2.getRefundOrderIdSet());
                        stats1.setRefund_order_ct(stats1.getRefundOrderIdSet().size() + 0L);
                        stats1.setRefund_amount(stats1.getRefund_amount().add(stats2.getRefund_amount()));
                        stats1.getPaidOrderIdSet().addAll(stats2.getPaidOrderIdSet());
                        stats1.setPaid_order_ct(stats1.getPaidOrderIdSet().size() + 0L);
                        stats1.setComment_ct(stats1.getComment_ct() + stats2.getComment_ct());
                        stats1.setGood_comment_ct(stats1.getGood_comment_ct() +
                                stats2.getGood_comment_ct());
                        return stats1;
                    }
                },
                new ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>() {
                    @Override
                    public void process(Long aLong, ProcessWindowFunction<ProductStats, ProductStats, Long, TimeWindow>.Context context, Iterable<ProductStats> productStatsIterable, Collector<ProductStats> out) throws Exception {
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        for (ProductStats productStats : productStatsIterable) {
                            productStats.setStt(simpleDateFormat.format(new Date(context.window().getStart())));
                            productStats.setEdt(simpleDateFormat.format(new Date(context.window().getEnd())));
                            productStats.setTs(new Date().getTime());
                            out.collect(productStats);
                        }
                    }
                }
        );

        //TODO 9.???????????????????????????
        //9.1 ????????????SKU
        SingleOutputStreamOperator<ProductStats> productStatsWithSku = AsyncDataStream.unorderedWait(
                reduceDS,
                new DimAsyncFunction<ProductStats>("DIM_SKU_INFO") {
                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSku_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJson) {
                        productStats.setSku_name(dimInfoJson.getString("SKU_NAME"));
                        productStats.setSku_price(dimInfoJson.getBigDecimal("PRICE"));
                        productStats.setCategory3_id(dimInfoJson.getLong("CATEGORY3_ID"));
                        productStats.setSpu_id(dimInfoJson.getLong("SPU_ID"));
                        productStats.setTm_id(dimInfoJson.getLong("TM_ID"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //9.2 ??????SPU??????
        SingleOutputStreamOperator<ProductStats> productStatsWithSpuDS = AsyncDataStream.unorderedWait(productStatsWithSku,
                new DimAsyncFunction<ProductStats>("DIM_SPU_INFO") {

                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getSpu_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJson) throws Exception {
                        productStats.setSpu_name(dimInfoJson.getString("SPU_NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //9.3 ??????????????????
        SingleOutputStreamOperator<ProductStats> productStatsWithTMDS = AsyncDataStream.unorderedWait(productStatsWithSpuDS,
                new DimAsyncFunction<ProductStats>("BASE_TRADEMARK") {

                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getTm_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJson) throws Exception {
                        productStats.setTm_name(dimInfoJson.getString("TM_NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //9.4 ??????????????????
        SingleOutputStreamOperator<ProductStats> productStatsWithCategoryDS = AsyncDataStream.unorderedWait(
                productStatsWithTMDS,
                new DimAsyncFunction<ProductStats>("DIM_BASE_CATEGORY3") {

                    @Override
                    public String getKey(ProductStats productStats) {
                        return productStats.getCategory3_id().toString();
                    }

                    @Override
                    public void join(ProductStats productStats, JSONObject dimInfoJson) throws Exception {
                        productStats.setCategory3_name(dimInfoJson.getString("NAME"));
                    }
                },
                60,
                TimeUnit.SECONDS
        );

        //TODO 10.??????????????????????????????ClickHouse???
        productStatsWithCategoryDS.addSink(ClickHouseUtil.<ProductStats>getJdbcSink("insert into product_stats values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"));

        //TODO 11.???????????????????????????kafka???dws???
        productStatsWithCategoryDS.map(productStat -> JSON.toJSONString(productStat, new SerializeConfig(true))).
                addSink(MyKafkaUtil.getKafkaSink("dws_product_stats"));

        env.execute();
    }
}
