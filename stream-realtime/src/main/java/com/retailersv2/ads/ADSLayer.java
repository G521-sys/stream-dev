package com.retailersv2.ads;

import com.retailersv2.dwd.DWDLayer;
import com.retailersv2.dws.DWSLayer;
import com.retailersv2.ods.ODSLayer;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * 应用数据层(ADS)
 * 生成最终业务指标，供前端展示和分析
 */
public class ADSLayer {
    // -------------------------- 1. MySQL连接配置（需根据实际环境修改）--------------------------
    // MySQL驱动（8.0+版本驱动类）
    private static final String MYSQL_DRIVER = "com.mysql.cj.jdbc.Driver";
    // MySQL连接URL（数据库名：retailers_ads，需提前创建）
    private static final String MYSQL_URL = "jdbc:mysql://cdh01:3306/realtime_v1?useSSL=false&serverTimezone=UTC&allowPublicKeyRetrieval=true";
    // MySQL用户名
    private static final String MYSQL_USER = "root";
    // MySQL密码
    private static final String MYSQL_PASSWORD = "000000";

    // 店铺客户核心指标
    public static class ShopCustomerADS {
        private String shopId;
        private String statDate;
        private int customerCount; // 店铺客户数

        // Getters and Setters
        public String getShopId() {
            return shopId;
        }

        public void setShopId(String shopId) {
            this.shopId = shopId;
        }

        public String getStatDate() {
            return statDate;
        }

        public void setStatDate(String statDate) {
            this.statDate = statDate;
        }

        public int getCustomerCount() {
            return customerCount;
        }

        public void setCustomerCount(int customerCount) {
            this.customerCount = customerCount;
        }

        @Override
        public String toString() {
            return "ShopCustomerADS{" +
                    "shopId='" + shopId + '\'' +
                    ", statDate='" + statDate + '\'' +
                    ", customerCount=" + customerCount +
                    '}';
        }
    }

    // 新访客户核心指标
    public static class NewVisitADS {
        private String shopId;
        private String statDate;
        private int newVisitCount; // 新访客户数
        private int newVisitPaidCount; // 新访成交数
        private int newVisitUnpaidCount; // 新访未成交数
        private double newVisitPaymentRate; // 新访支付转化率
        private BigDecimal newVisitPaymentAmount; // 新访支付总金额
        private double newVisitPaymentRatio; // 新访支付金额占比
        private BigDecimal newVisitAvgPrice; // 新访客单价
        private int newVisitFollowCount; // 新访粉丝数
        private int newVisitMemberCount; // 新访会员数

        // Getters and Setters
        public String getShopId() {
            return shopId;
        }

        public void setShopId(String shopId) {
            this.shopId = shopId;
        }

        public String getStatDate() {
            return statDate;
        }

        public void setStatDate(String statDate) {
            this.statDate = statDate;
        }

        public int getNewVisitCount() {
            return newVisitCount;
        }

        public void setNewVisitCount(int newVisitCount) {
            this.newVisitCount = newVisitCount;
        }

        public int getNewVisitPaidCount() {
            return newVisitPaidCount;
        }

        public void setNewVisitPaidCount(int newVisitPaidCount) {
            this.newVisitPaidCount = newVisitPaidCount;
        }

        public int getNewVisitUnpaidCount() {
            return newVisitUnpaidCount;
        }

        public void setNewVisitUnpaidCount(int newVisitUnpaidCount) {
            this.newVisitUnpaidCount = newVisitUnpaidCount;
        }

        public double getNewVisitPaymentRate() {
            return newVisitPaymentRate;
        }

        public void setNewVisitPaymentRate(double newVisitPaymentRate) {
            this.newVisitPaymentRate = newVisitPaymentRate;
        }

        public BigDecimal getNewVisitPaymentAmount() {
            return newVisitPaymentAmount;
        }

        public void setNewVisitPaymentAmount(BigDecimal newVisitPaymentAmount) {
            this.newVisitPaymentAmount = newVisitPaymentAmount;
        }

        public double getNewVisitPaymentRatio() {
            return newVisitPaymentRatio;
        }

        public void setNewVisitPaymentRatio(double newVisitPaymentRatio) {
            this.newVisitPaymentRatio = newVisitPaymentRatio;
        }

        public BigDecimal getNewVisitAvgPrice() {
            return newVisitAvgPrice;
        }

        public void setNewVisitAvgPrice(BigDecimal newVisitAvgPrice) {
            this.newVisitAvgPrice = newVisitAvgPrice;
        }

        public int getNewVisitFollowCount() {
            return newVisitFollowCount;
        }

        public void setNewVisitFollowCount(int newVisitFollowCount) {
            this.newVisitFollowCount = newVisitFollowCount;
        }

        public int getNewVisitMemberCount() {
            return newVisitMemberCount;
        }

        public void setNewVisitMemberCount(int newVisitMemberCount) {
            this.newVisitMemberCount = newVisitMemberCount;
        }

        @Override
        public String toString() {
            return "NewVisitADS{" +
                    "shopId='" + shopId + '\'' +
                    ", statDate='" + statDate + '\'' +
                    ", newVisitCount=" + newVisitCount +
                    ", newVisitPaidCount=" + newVisitPaidCount +
                    ", newVisitPaymentRate=" + newVisitPaymentRate +
                    ", newVisitAvgPrice=" + newVisitAvgPrice +
                    ", newVisitFollowCount=" + newVisitFollowCount +
                    ", newVisitMemberCount=" + newVisitMemberCount +
                    '}';
        }
    }

    // 回访客户核心指标
    public static class ReturnVisitADS {
        private String shopId;
        private String statDate;
        private int returnVisitCount; // 未购客户回访数
        private int returnVisitPaidCount; // 回访成交数
        private int returnVisitUnpaidCount; // 回访未成交数
        private double returnVisitPaymentRate; // 回访支付转化率
        private BigDecimal returnVisitPaymentAmount; // 回访支付总金额
        private double returnVisitPaymentRatio; // 回访支付金额占比
        private BigDecimal returnVisitAvgPrice; // 回访客单价
        private int returnVisitFollowCount; // 回访粉丝数
        private int returnVisitMemberCount; // 回访会员数

        // Getters and Setters
        public String getShopId() {
            return shopId;
        }

        public void setShopId(String shopId) {
            this.shopId = shopId;
        }

        public String getStatDate() {
            return statDate;
        }

        public void setStatDate(String statDate) {
            this.statDate = statDate;
        }

        public int getReturnVisitCount() {
            return returnVisitCount;
        }

        public void setReturnVisitCount(int returnVisitCount) {
            this.returnVisitCount = returnVisitCount;
        }

        public int getReturnVisitPaidCount() {
            return returnVisitPaidCount;
        }

        public void setReturnVisitPaidCount(int returnVisitPaidCount) {
            this.returnVisitPaidCount = returnVisitPaidCount;
        }

        public int getReturnVisitUnpaidCount() {
            return returnVisitUnpaidCount;
        }

        public void setReturnVisitUnpaidCount(int returnVisitUnpaidCount) {
            this.returnVisitUnpaidCount = returnVisitUnpaidCount;
        }

        public double getReturnVisitPaymentRate() {
            return returnVisitPaymentRate;
        }

        public void setReturnVisitPaymentRate(double returnVisitPaymentRate) {
            this.returnVisitPaymentRate = returnVisitPaymentRate;
        }

        public BigDecimal getReturnVisitPaymentAmount() {
            return returnVisitPaymentAmount;
        }

        public void setReturnVisitPaymentAmount(BigDecimal returnVisitPaymentAmount) {
            this.returnVisitPaymentAmount = returnVisitPaymentAmount;
        }

        public double getReturnVisitPaymentRatio() {
            return returnVisitPaymentRatio;
        }

        public void setReturnVisitPaymentRatio(double returnVisitPaymentRatio) {
            this.returnVisitPaymentRatio = returnVisitPaymentRatio;
        }

        public BigDecimal getReturnVisitAvgPrice() {
            return returnVisitAvgPrice;
        }

        public void setReturnVisitAvgPrice(BigDecimal returnVisitAvgPrice) {
            this.returnVisitAvgPrice = returnVisitAvgPrice;
        }

        public int getReturnVisitFollowCount() {
            return returnVisitFollowCount;
        }

        public void setReturnVisitFollowCount(int returnVisitFollowCount) {
            this.returnVisitFollowCount = returnVisitFollowCount;
        }

        public int getReturnVisitMemberCount() {
            return returnVisitMemberCount;
        }

        public void setReturnVisitMemberCount(int returnVisitMemberCount) {
            this.returnVisitMemberCount = returnVisitMemberCount;
        }

        @Override
        public String toString() {
            return "ReturnVisitADS{" +
                    "shopId='" + shopId + '\'' +
                    ", statDate='" + statDate + '\'' +
                    ", returnVisitCount=" + returnVisitCount +
                    ", returnVisitPaidCount=" + returnVisitPaidCount +
                    ", returnVisitPaymentRate=" + returnVisitPaymentRate +
                    ", returnVisitAvgPrice=" + returnVisitAvgPrice +
                    ", returnVisitFollowCount=" + returnVisitFollowCount +
                    ", returnVisitMemberCount=" + returnVisitMemberCount +
                    '}';
        }
    }

    // 老客户核心指标
    public static class OldCustomerADS {
        private String shopId;
        private String statDate;
        private int oldCustomerRevisitCount; // 已购客户回访数
        private int oldCustomerRepurchaseCount; // 老客复购数
        private int oldCustomerNonRepurchaseCount; // 老客未复购数
        private double oldCustomerPaymentRate; // 老客支付转化率
        private BigDecimal oldCustomerPaymentAmount; // 老客支付总金额
        private double oldCustomerPaymentRatio; // 老客支付金额占比
        private BigDecimal oldCustomerAvgPrice; // 老客户单价
        private int oldCustomerFollowCount; // 老客粉丝数
        private int oldCustomerMemberCount; // 老客会员数

        // Getters and Setters
        public String getShopId() {
            return shopId;
        }

        public void setShopId(String shopId) {
            this.shopId = shopId;
        }

        public String getStatDate() {
            return statDate;
        }

        public void setStatDate(String statDate) {
            this.statDate = statDate;
        }

        public int getOldCustomerRevisitCount() {
            return oldCustomerRevisitCount;
        }

        public void setOldCustomerRevisitCount(int oldCustomerRevisitCount) {
            this.oldCustomerRevisitCount = oldCustomerRevisitCount;
        }

        public int getOldCustomerRepurchaseCount() {
            return oldCustomerRepurchaseCount;
        }

        public void setOldCustomerRepurchaseCount(int oldCustomerRepurchaseCount) {
            this.oldCustomerRepurchaseCount = oldCustomerRepurchaseCount;
        }

        public int getOldCustomerNonRepurchaseCount() {
            return oldCustomerNonRepurchaseCount;
        }

        public void setOldCustomerNonRepurchaseCount(int oldCustomerNonRepurchaseCount) {
            this.oldCustomerNonRepurchaseCount = oldCustomerNonRepurchaseCount;
        }

        public double getOldCustomerPaymentRate() {
            return oldCustomerPaymentRate;
        }

        public void setOldCustomerPaymentRate(double oldCustomerPaymentRate) {
            this.oldCustomerPaymentRate = oldCustomerPaymentRate;
        }

        public BigDecimal getOldCustomerPaymentAmount() {
            return oldCustomerPaymentAmount;
        }

        public void setOldCustomerPaymentAmount(BigDecimal oldCustomerPaymentAmount) {
            this.oldCustomerPaymentAmount = oldCustomerPaymentAmount;
        }

        public double getOldCustomerPaymentRatio() {
            return oldCustomerPaymentRatio;
        }

        public void setOldCustomerPaymentRatio(double oldCustomerPaymentRatio) {
            this.oldCustomerPaymentRatio = oldCustomerPaymentRatio;
        }

        public BigDecimal getOldCustomerAvgPrice() {
            return oldCustomerAvgPrice;
        }

        public void setOldCustomerAvgPrice(BigDecimal oldCustomerAvgPrice) {
            this.oldCustomerAvgPrice = oldCustomerAvgPrice;
        }

        public int getOldCustomerFollowCount() {
            return oldCustomerFollowCount;
        }

        public void setOldCustomerFollowCount(int oldCustomerFollowCount) {
            this.oldCustomerFollowCount = oldCustomerFollowCount;
        }

        public int getOldCustomerMemberCount() {
            return oldCustomerMemberCount;
        }

        public void setOldCustomerMemberCount(int oldCustomerMemberCount) {
            this.oldCustomerMemberCount = oldCustomerMemberCount;
        }

        @Override
        public String toString() {
            return "OldCustomerADS{" +
                    "shopId='" + shopId + '\'' +
                    ", statDate='" + statDate + '\'' +
                    ", oldCustomerRevisitCount=" + oldCustomerRevisitCount +
                    ", oldCustomerRepurchaseCount=" + oldCustomerRepurchaseCount +
                    ", oldCustomerPaymentRate=" + oldCustomerPaymentRate +
                    ", oldCustomerAvgPrice=" + oldCustomerAvgPrice +
                    ", oldCustomerFollowCount=" + oldCustomerFollowCount +
                    ", oldCustomerMemberCount=" + oldCustomerMemberCount +
                    '}';
        }
    }

    // 创建ADS层指标
    public static void createMetrics(StreamExecutionEnvironment env, DWSLayer.DWSStreams dwsStreams) {
        // 1. 店铺客户数指标
        DataStream<ShopCustomerADS> shopCustomerMetrics = dwsStreams.getShopCustomerStream()
                .map((MapFunction<DWSLayer.ShopCustomerDWS, ShopCustomerADS>) dws -> {
                    ShopCustomerADS ads = new ShopCustomerADS();
                    ads.setShopId(dws.getShopId());
                    ads.setStatDate(dws.getStatDate());
                    ads.setCustomerCount(dws.getCustomerCount());
                    return ads;
                })
                .name("Shop Customer Metrics")
                .uid("ads-shop-customer");
        // 店铺客户指标写入MySQL
        shopCustomerMetrics.addSink(
                JdbcSink.sink(
                        "INSERT INTO ads_shop_customer (shop_id, stat_date, customer_count) VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE customer_count = VALUES(customer_count)",
                        (JdbcStatementBuilder<ShopCustomerADS>) (ps, ads) -> {
                            ps.setString(1, ads.getShopId());
                            ps.setString(2, ads.getStatDate());
                            ps.setInt(3, ads.getCustomerCount());
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(100)
                                .withBatchIntervalMs(2000)
                                .withMaxRetries(3)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(MYSQL_URL)
                                .withDriverName(MYSQL_DRIVER)
                                .withUsername(MYSQL_USER)
                                .withPassword(MYSQL_PASSWORD)
                                .build()
                )
        ).name("ShopCustomerADS Sink").uid("sink-shop-customer-ads");

        // 2. 新访客户指标
        DataStream<NewVisitADS> newVisitMetrics = dwsStreams.getNewVisitStream()
                .keyBy(new KeySelector<DWSLayer.NewVisitDWS, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(DWSLayer.NewVisitDWS visit) throws Exception {
                        return new Tuple2<>(visit.getShopId(), visit.getStatDate());
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .aggregate(new AggregateFunction<DWSLayer.NewVisitDWS, NewVisitADS, NewVisitADS>() {
                    @Override
                    public NewVisitADS createAccumulator() {
                        NewVisitADS accumulator = new NewVisitADS();
                        accumulator.setNewVisitPaymentAmount(BigDecimal.ZERO);
                        return accumulator;
                    }

                    @Override
                    public NewVisitADS add(DWSLayer.NewVisitDWS value, NewVisitADS accumulator) {
                        // 初始化累加器信息
                        if (accumulator.getShopId() == null) {
                            accumulator.setShopId(value.getShopId());
                            accumulator.setStatDate(value.getStatDate());
                        }

                        // 累加计数
                        accumulator.setNewVisitCount(accumulator.getNewVisitCount() + value.getVisitCount());
                        accumulator.setNewVisitPaidCount(accumulator.getNewVisitPaidCount() + value.getPaidCount());
                        accumulator.setNewVisitUnpaidCount(accumulator.getNewVisitUnpaidCount() + value.getUnpaidCount());
                        accumulator.setNewVisitPaymentAmount(
                                accumulator.getNewVisitPaymentAmount().add(value.getPaymentAmount()));

                        // 累加粉丝数
                        if (value.isFollowed()) {
                            accumulator.setNewVisitFollowCount(accumulator.getNewVisitFollowCount() + 1);
                        }

                        // 判断是否为会员（交易额>5000或交易次数>3）
                        if (value.getTotalPurchaseAmount().compareTo(new BigDecimal(5000)) > 0 ||
                            value.getTotalPurchaseCount() > 3) {
                            accumulator.setNewVisitMemberCount(accumulator.getNewVisitMemberCount() + 1);
                        }

                        return accumulator;
                    }

                    @Override
                    public NewVisitADS getResult(NewVisitADS accumulator) {
                        // 计算转化率
                        if (accumulator.getNewVisitCount() > 0) {
                            accumulator.setNewVisitPaymentRate(
                                    (double) accumulator.getNewVisitPaidCount() / accumulator.getNewVisitCount());
                        }

                        // 计算客单价
                        if (accumulator.getNewVisitPaidCount() > 0) {
                            accumulator.setNewVisitAvgPrice(
                                    accumulator.getNewVisitPaymentAmount()
                                            .divide(new BigDecimal(accumulator.getNewVisitPaidCount()), 2, RoundingMode.HALF_UP));
                        }

                        return accumulator;
                    }

                    @Override
                    public NewVisitADS merge(NewVisitADS a, NewVisitADS b) {
                        NewVisitADS merged = new NewVisitADS();
                        merged.setShopId(a.getShopId());
                        merged.setStatDate(a.getStatDate());
                        merged.setNewVisitCount(a.getNewVisitCount() + b.getNewVisitCount());
                        merged.setNewVisitPaidCount(a.getNewVisitPaidCount() + b.getNewVisitPaidCount());
                        merged.setNewVisitUnpaidCount(a.getNewVisitUnpaidCount() + b.getNewVisitUnpaidCount());
                        merged.setNewVisitPaymentAmount(a.getNewVisitPaymentAmount().add(b.getNewVisitPaymentAmount()));
                        merged.setNewVisitFollowCount(a.getNewVisitFollowCount() + b.getNewVisitFollowCount());
                        merged.setNewVisitMemberCount(a.getNewVisitMemberCount() + b.getNewVisitMemberCount());

                        // 重新计算比率
                        if (merged.getNewVisitCount() > 0) {
                            merged.setNewVisitPaymentRate(
                                    (double) merged.getNewVisitPaidCount() / merged.getNewVisitCount());
                        }

                        if (merged.getNewVisitPaidCount() > 0) {
                            merged.setNewVisitAvgPrice(
                                    merged.getNewVisitPaymentAmount()
                                            .divide(new BigDecimal(merged.getNewVisitPaidCount()), 2, RoundingMode.HALF_UP));
                        }

                        return merged;
                    }
                })
                .name("New Visit Metrics")
                .uid("ads-new-visit");


        // 新访客户指标写入MySQL
        newVisitMetrics.addSink(
                JdbcSink.sink(
                        "INSERT INTO ads_new_visit (" +
                                "shop_id, stat_date, new_visit_count, new_visit_paid_count, new_visit_unpaid_count, " +
                                "new_visit_payment_rate, new_visit_payment_amount, new_visit_avg_price, " +
                                "new_visit_follow_count, new_visit_member_count" +
                                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                                "ON DUPLICATE KEY UPDATE " +
                                "new_visit_count = VALUES(new_visit_count), " +
                                "new_visit_paid_count = VALUES(new_visit_paid_count), " +
                                "new_visit_unpaid_count = VALUES(new_visit_unpaid_count), " +
                                "new_visit_payment_rate = VALUES(new_visit_payment_rate), " +
                                "new_visit_payment_amount = VALUES(new_visit_payment_amount), " +
                                "new_visit_avg_price = VALUES(new_visit_avg_price), " +
                                "new_visit_follow_count = VALUES(new_visit_follow_count), " +
                                "new_visit_member_count = VALUES(new_visit_member_count)",
                        (JdbcStatementBuilder<NewVisitADS>) (ps, ads) -> {
                            ps.setString(1, ads.getShopId());
                            ps.setString(2, ads.getStatDate());
                            ps.setInt(3, ads.getNewVisitCount());
                            ps.setInt(4, ads.getNewVisitPaidCount());
                            ps.setInt(5, ads.getNewVisitUnpaidCount());
                            ps.setDouble(6, ads.getNewVisitPaymentRate());
                            ps.setBigDecimal(7, ads.getNewVisitPaymentAmount());
                            ps.setBigDecimal(8, ads.getNewVisitAvgPrice() != null ? ads.getNewVisitAvgPrice() : BigDecimal.ZERO);
                            ps.setInt(9, ads.getNewVisitFollowCount());
                            ps.setInt(10, ads.getNewVisitMemberCount());
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(100)
                                .withBatchIntervalMs(2000)
                                .withMaxRetries(3)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(MYSQL_URL)
                                .withDriverName(MYSQL_DRIVER)
                                .withUsername(MYSQL_USER)
                                .withPassword(MYSQL_PASSWORD)
                                .build()
                )
        ).name("NewVisitADS Sink").uid("sink-new-visit-ads");


        // 3. 回访客户指标（实现逻辑类似新访指标）
        DataStream<ReturnVisitADS> returnVisitMetrics = dwsStreams.getReturnVisitStream()
                .keyBy(new KeySelector<DWSLayer.ReturnVisitDWS, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(DWSLayer.ReturnVisitDWS visit) throws Exception {
                        return new Tuple2<>(visit.getShopId(), visit.getStatDate());
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .aggregate(new ReturnVisitAggregate())
                .name("Return Visit Metrics")
                .uid("ads-return-visit");
        // 回访客户指标写入MySQL
        returnVisitMetrics.addSink(
                JdbcSink.sink(
                        "INSERT INTO ads_return_visit (" +
                                "shop_id, stat_date, return_visit_count, return_visit_paid_count, return_visit_unpaid_count, " +
                                "return_visit_payment_rate, return_visit_payment_amount, return_visit_avg_price, " +
                                "return_visit_follow_count, return_visit_member_count" +
                                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                                "ON DUPLICATE KEY UPDATE " +
                                "return_visit_count = VALUES(return_visit_count), " +
                                "return_visit_paid_count = VALUES(return_visit_paid_count), " +
                                "return_visit_unpaid_count = VALUES(return_visit_unpaid_count), " +
                                "return_visit_payment_rate = VALUES(return_visit_payment_rate), " +
                                "return_visit_payment_amount = VALUES(return_visit_payment_amount), " +
                                "return_visit_avg_price = VALUES(return_visit_avg_price), " +
                                "return_visit_follow_count = VALUES(return_visit_follow_count), " +
                                "return_visit_member_count = VALUES(return_visit_member_count)",
                        (JdbcStatementBuilder<ReturnVisitADS>) (ps, ads) -> {
                            ps.setString(1, ads.getShopId());
                            ps.setString(2, ads.getStatDate());
                            ps.setInt(3, ads.getReturnVisitCount());
                            ps.setInt(4, ads.getReturnVisitPaidCount());
                            ps.setInt(5, ads.getReturnVisitUnpaidCount());
                            ps.setDouble(6, ads.getReturnVisitPaymentRate());
                            ps.setBigDecimal(7, ads.getReturnVisitPaymentAmount());
                            ps.setBigDecimal(8, ads.getReturnVisitAvgPrice() != null ? ads.getReturnVisitAvgPrice() : BigDecimal.ZERO);
                            ps.setInt(9, ads.getReturnVisitFollowCount());
                            ps.setInt(10, ads.getReturnVisitMemberCount());
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(100)
                                .withBatchIntervalMs(2000)
                                .withMaxRetries(3)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(MYSQL_URL)
                                .withDriverName(MYSQL_DRIVER)
                                .withUsername(MYSQL_USER)
                                .withPassword(MYSQL_PASSWORD)
                                .build()
                )
        ).name("ReturnVisitADS Sink").uid("sink-return-visit-ads");
        // 4. 老客户指标（实现逻辑类似）
        DataStream<OldCustomerADS> oldCustomerMetrics = dwsStreams.getOldCustomerStream()
                .keyBy(new KeySelector<DWSLayer.OldCustomerDWS, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(DWSLayer.OldCustomerDWS customer) throws Exception {
                        return new Tuple2<>(customer.getShopId(), customer.getStatDate());
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(2)))
                .aggregate(new OldCustomerAggregate())
                .name("Old Customer Metrics")
                .uid("ads-old-customer");

        // 老客户指标写入MySQL
        oldCustomerMetrics.addSink(
                JdbcSink.sink(
                        "INSERT INTO ads_old_customer (" +
                                "shop_id, stat_date, old_customer_revisit_count, old_customer_repurchase_count, old_customer_non_repurchase_count, " +
                                "old_customer_payment_rate, old_customer_payment_amount, old_customer_avg_price, " +
                                "old_customer_follow_count, old_customer_member_count" +
                                ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                                "ON DUPLICATE KEY UPDATE " +
                                "old_customer_revisit_count = VALUES(old_customer_revisit_count), " +
                                "old_customer_repurchase_count = VALUES(old_customer_repurchase_count), " +
                                "old_customer_non_repurchase_count = VALUES(old_customer_non_repurchase_count), " +
                                "old_customer_payment_rate = VALUES(old_customer_payment_rate), " +
                                "old_customer_payment_amount = VALUES(old_customer_payment_amount), " +
                                "old_customer_avg_price = VALUES(old_customer_avg_price), " +
                                "old_customer_follow_count = VALUES(old_customer_follow_count), " +
                                "old_customer_member_count = VALUES(old_customer_member_count)",
                        (JdbcStatementBuilder<OldCustomerADS>) (ps, ads) -> {
                            ps.setString(1, ads.getShopId());
                            ps.setString(2, ads.getStatDate());
                            ps.setInt(3, ads.getOldCustomerRevisitCount());
                            ps.setInt(4, ads.getOldCustomerRepurchaseCount());
                            ps.setInt(5, ads.getOldCustomerNonRepurchaseCount());
                            ps.setDouble(6, ads.getOldCustomerPaymentRate());
                            ps.setBigDecimal(7, ads.getOldCustomerPaymentAmount());
                            ps.setBigDecimal(8, ads.getOldCustomerAvgPrice() != null ? ads.getOldCustomerAvgPrice() : BigDecimal.ZERO);
                            ps.setInt(9, ads.getOldCustomerFollowCount());
                            ps.setInt(10, ads.getOldCustomerMemberCount());
                        },
                        JdbcExecutionOptions.builder()
                                .withBatchSize(100)
                                .withBatchIntervalMs(2000)
                                .withMaxRetries(3)
                                .build(),
                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl(MYSQL_URL)
                                .withDriverName(MYSQL_DRIVER)
                                .withUsername(MYSQL_USER)
                                .withPassword(MYSQL_PASSWORD)
                                .build()
                )
        ).name("OldCustomerADS Sink").uid("sink-old-customer-ads");

        // 输出指标结果（实际应用中会输出到数据库或可视化系统）
        shopCustomerMetrics.print("店铺客户数指标:");
        newVisitMetrics.print("新访客户指标:");
        returnVisitMetrics.print("回访客户指标:");
        oldCustomerMetrics.print("老客户指标:");
    }

    // 回访客户指标聚合器
    public static class ReturnVisitAggregate implements AggregateFunction<DWSLayer.ReturnVisitDWS, ReturnVisitADS, ReturnVisitADS> {
        @Override
        public ReturnVisitADS createAccumulator() {
            ReturnVisitADS accumulator = new ReturnVisitADS();
            accumulator.setReturnVisitPaymentAmount(BigDecimal.ZERO);
            return accumulator;
        }

        @Override
        public ReturnVisitADS add(DWSLayer.ReturnVisitDWS value, ReturnVisitADS accumulator) {
            if (accumulator.getShopId() == null) {
                accumulator.setShopId(value.getShopId());
                accumulator.setStatDate(value.getStatDate());
            }

            accumulator.setReturnVisitCount(accumulator.getReturnVisitCount() + value.getVisitCount());
            accumulator.setReturnVisitPaidCount(accumulator.getReturnVisitPaidCount() + value.getPaidCount());
            accumulator.setReturnVisitUnpaidCount(accumulator.getReturnVisitUnpaidCount() + value.getUnpaidCount());
            accumulator.setReturnVisitPaymentAmount(
                    accumulator.getReturnVisitPaymentAmount().add(value.getPaymentAmount()));

            if (value.isFollowed()) {
                accumulator.setReturnVisitFollowCount(accumulator.getReturnVisitFollowCount() + 1);
            }

            if (value.getTotalPurchaseAmount().compareTo(new BigDecimal(5000)) > 0 ||
                value.getTotalPurchaseCount() > 3) {
                accumulator.setReturnVisitMemberCount(accumulator.getReturnVisitMemberCount() + 1);
            }

            return accumulator;
        }

        @Override
        public ReturnVisitADS getResult(ReturnVisitADS accumulator) {
            if (accumulator.getReturnVisitCount() > 0) {
                accumulator.setReturnVisitPaymentRate(
                        (double) accumulator.getReturnVisitPaidCount() / accumulator.getReturnVisitCount());
            }

            if (accumulator.getReturnVisitPaidCount() > 0) {
                accumulator.setReturnVisitAvgPrice(
                        accumulator.getReturnVisitPaymentAmount()
                                .divide(new BigDecimal(accumulator.getReturnVisitPaidCount()), 2, RoundingMode.HALF_UP));
            }

            return accumulator;
        }

        @Override
        public ReturnVisitADS merge(ReturnVisitADS a, ReturnVisitADS b) {
            ReturnVisitADS merged = new ReturnVisitADS();
            merged.setShopId(a.getShopId());
            merged.setStatDate(a.getStatDate());
            merged.setReturnVisitCount(a.getReturnVisitCount() + b.getReturnVisitCount());
            merged.setReturnVisitPaidCount(a.getReturnVisitPaidCount() + b.getReturnVisitPaidCount());
            merged.setReturnVisitUnpaidCount(a.getReturnVisitUnpaidCount() + b.getReturnVisitUnpaidCount());
            merged.setReturnVisitPaymentAmount(a.getReturnVisitPaymentAmount().add(b.getReturnVisitPaymentAmount()));
            merged.setReturnVisitFollowCount(a.getReturnVisitFollowCount() + b.getReturnVisitFollowCount());
            merged.setReturnVisitMemberCount(a.getReturnVisitMemberCount() + b.getReturnVisitMemberCount());

            if (merged.getReturnVisitCount() > 0) {
                merged.setReturnVisitPaymentRate(
                        (double) merged.getReturnVisitPaidCount() / merged.getReturnVisitCount());
            }

            if (merged.getReturnVisitPaidCount() > 0) {
                merged.setReturnVisitAvgPrice(
                        merged.getReturnVisitPaymentAmount()
                                .divide(new BigDecimal(merged.getReturnVisitPaidCount()), 2, RoundingMode.HALF_UP));
            }

            return merged;
        }
    }

    // 老客户指标聚合器
    public static class OldCustomerAggregate implements AggregateFunction<DWSLayer.OldCustomerDWS, OldCustomerADS, OldCustomerADS> {
        @Override
        public OldCustomerADS createAccumulator() {
            OldCustomerADS accumulator = new OldCustomerADS();
            accumulator.setOldCustomerPaymentAmount(BigDecimal.ZERO);
            return accumulator;
        }

        @Override
        public OldCustomerADS add(DWSLayer.OldCustomerDWS value, OldCustomerADS accumulator) {
            if (accumulator.getShopId() == null) {
                accumulator.setShopId(value.getShopId());
                accumulator.setStatDate(value.getStatDate());
            }

            accumulator.setOldCustomerRevisitCount(accumulator.getOldCustomerRevisitCount() + value.getRevisitCount());
            accumulator.setOldCustomerRepurchaseCount(accumulator.getOldCustomerRepurchaseCount() + value.getRepurchaseCount());
            accumulator.setOldCustomerNonRepurchaseCount(accumulator.getOldCustomerNonRepurchaseCount() + value.getNonRepurchaseCount());
            accumulator.setOldCustomerPaymentAmount(
                    accumulator.getOldCustomerPaymentAmount().add(value.getPaymentAmount()));

            if (value.isFollowed()) {
                accumulator.setOldCustomerFollowCount(accumulator.getOldCustomerFollowCount() + 1);
            }

            if (value.getTotalPurchaseAmount().compareTo(new BigDecimal(5000)) > 0 ||
                value.getTotalPurchaseCount() > 3) {
                accumulator.setOldCustomerMemberCount(accumulator.getOldCustomerMemberCount() + 1);
            }

            return accumulator;
        }

        @Override
        public OldCustomerADS getResult(OldCustomerADS accumulator) {
            if (accumulator.getOldCustomerRevisitCount() > 0) {
                accumulator.setOldCustomerPaymentRate(
                        (double) accumulator.getOldCustomerRepurchaseCount() / accumulator.getOldCustomerRevisitCount());
            }

            if (accumulator.getOldCustomerRepurchaseCount() > 0) {
                accumulator.setOldCustomerAvgPrice(
                        accumulator.getOldCustomerPaymentAmount()
                                .divide(new BigDecimal(accumulator.getOldCustomerRepurchaseCount()), 2, RoundingMode.HALF_UP));
            }

            return accumulator;
        }

        @Override
        public OldCustomerADS merge(OldCustomerADS a, OldCustomerADS b) {
            OldCustomerADS merged = new OldCustomerADS();
            merged.setShopId(a.getShopId());
            merged.setStatDate(a.getStatDate());
            merged.setOldCustomerRevisitCount(a.getOldCustomerRevisitCount() + b.getOldCustomerRevisitCount());
            merged.setOldCustomerRepurchaseCount(a.getOldCustomerRepurchaseCount() + b.getOldCustomerRepurchaseCount());
            merged.setOldCustomerNonRepurchaseCount(a.getOldCustomerNonRepurchaseCount() + b.getOldCustomerNonRepurchaseCount());
            merged.setOldCustomerPaymentAmount(a.getOldCustomerPaymentAmount().add(b.getOldCustomerPaymentAmount()));
            merged.setOldCustomerFollowCount(a.getOldCustomerFollowCount() + b.getOldCustomerFollowCount());
            merged.setOldCustomerMemberCount(a.getOldCustomerMemberCount() + b.getOldCustomerMemberCount());

            if (merged.getOldCustomerRevisitCount() > 0) {
                merged.setOldCustomerPaymentRate(
                        (double) merged.getOldCustomerRepurchaseCount() / merged.getOldCustomerRevisitCount());
            }

            if (merged.getOldCustomerRepurchaseCount() > 0) {
                merged.setOldCustomerAvgPrice(
                        merged.getOldCustomerPaymentAmount()
                                .divide(new BigDecimal(merged.getOldCustomerRepurchaseCount()), 2, RoundingMode.HALF_UP));
            }

            return merged;
        }
    }

    // 主方法，用于启动整个数据仓库
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 构建各层数据流
        DataStream<ODSLayer.ODSUserAction> odsStream = ODSLayer.createStream(env);
        DWDLayer.DWDStreams dwdStreams = DWDLayer.createStreams(odsStream);
        DWSLayer.DWSStreams dwsStreams = DWSLayer.createStreams(dwdStreams);

        // 3. 生成并输出ADS层指标
        createMetrics(env, dwsStreams);

        // 执行任务
        env.execute("Shop Customer Behavior Data Warehouse");
    }
}
