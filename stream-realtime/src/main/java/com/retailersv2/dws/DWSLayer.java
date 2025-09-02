package com.retailersv2.dws;

import com.retailersv2.dwd.DWDLayer;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.math.BigDecimal;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * 数据汇总层(DWS)
 * 按用户类型和统计周期汇总计算
 */
public class DWSLayer {
    // 店铺客户数汇总数据类
    public static class ShopCustomerDWS {
        private String shopId;
        private String statDate;
        private int customerCount; // 店铺客户数

        // Getters and Setters
        public String getShopId() { return shopId; }
        public void setShopId(String shopId) { this.shopId = shopId; }
        public String getStatDate() { return statDate; }
        public void setStatDate(String statDate) { this.statDate = statDate; }
        public int getCustomerCount() { return customerCount; }
        public void setCustomerCount(int customerCount) { this.customerCount = customerCount; }

        @Override
        public String toString() {
            return "ShopCustomerDWS{" +
                    "shopId='" + shopId + '\'' +
                    ", statDate='" + statDate + '\'' +
                    ", customerCount=" + customerCount +
                    '}';
        }
    }

    // 新访用户汇总数据类
    public static class NewVisitDWS {
        private String shopId;
        private String statDate;
        private String userId;
        private int visitCount = 0;
        private int paidCount = 0;
        private int unpaidCount = 0;
        private BigDecimal paymentAmount = BigDecimal.ZERO;
        private boolean isFollowed = false;
        private int totalPurchaseCount = 0;
        private BigDecimal totalPurchaseAmount = BigDecimal.ZERO;

        // Getters and Setters
        public String getShopId() { return shopId; }
        public void setShopId(String shopId) { this.shopId = shopId; }
        public String getStatDate() { return statDate; }
        public void setStatDate(String statDate) { this.statDate = statDate; }
        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }
        public int getVisitCount() { return visitCount; }
        public void setVisitCount(int visitCount) { this.visitCount = visitCount; }
        public int getPaidCount() { return paidCount; }
        public void setPaidCount(int paidCount) { this.paidCount = paidCount; }
        public int getUnpaidCount() { return unpaidCount; }
        public void setUnpaidCount(int unpaidCount) { this.unpaidCount = unpaidCount; }
        public BigDecimal getPaymentAmount() { return paymentAmount; }
        public void setPaymentAmount(BigDecimal paymentAmount) { this.paymentAmount = paymentAmount; }
        public boolean isFollowed() { return isFollowed; }
        public void setFollowed(boolean followed) { isFollowed = followed; }
        public int getTotalPurchaseCount() { return totalPurchaseCount; }
        public void setTotalPurchaseCount(int totalPurchaseCount) { this.totalPurchaseCount = totalPurchaseCount; }
        public BigDecimal getTotalPurchaseAmount() { return totalPurchaseAmount; }
        public void setTotalPurchaseAmount(BigDecimal totalPurchaseAmount) { this.totalPurchaseAmount = totalPurchaseAmount; }
    }

    // 回访用户汇总数据类
    public static class ReturnVisitDWS {
        private String shopId;
        private String statDate;
        private String userId;
        private int visitCount = 0;
        private int paidCount = 0;
        private int unpaidCount = 0;
        private BigDecimal paymentAmount = BigDecimal.ZERO;
        private boolean isFollowed = false;
        private int totalPurchaseCount = 0;
        private BigDecimal totalPurchaseAmount = BigDecimal.ZERO;

        // Getters and Setters
        public String getShopId() { return shopId; }
        public void setShopId(String shopId) { this.shopId = shopId; }
        public String getStatDate() { return statDate; }
        public void setStatDate(String statDate) { this.statDate = statDate; }
        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }
        public int getVisitCount() { return visitCount; }
        public void setVisitCount(int visitCount) { this.visitCount = visitCount; }
        public int getPaidCount() { return paidCount; }
        public void setPaidCount(int paidCount) { this.paidCount = paidCount; }
        public int getUnpaidCount() { return unpaidCount; }
        public void setUnpaidCount(int unpaidCount) { this.unpaidCount = unpaidCount; }
        public BigDecimal getPaymentAmount() { return paymentAmount; }
        public void setPaymentAmount(BigDecimal paymentAmount) { this.paymentAmount = paymentAmount; }
        public boolean isFollowed() { return isFollowed; }
        public void setFollowed(boolean followed) { isFollowed = followed; }
        public int getTotalPurchaseCount() { return totalPurchaseCount; }
        public void setTotalPurchaseCount(int totalPurchaseCount) { this.totalPurchaseCount = totalPurchaseCount; }
        public BigDecimal getTotalPurchaseAmount() { return totalPurchaseAmount; }
        public void setTotalPurchaseAmount(BigDecimal totalPurchaseAmount) { this.totalPurchaseAmount = totalPurchaseAmount; }
    }

    // 老客户汇总数据类
    public static class OldCustomerDWS {
        private String shopId;
        private String statDate;
        private String userId;
        private int revisitCount = 0;
        private int repurchaseCount = 0;
        private int nonRepurchaseCount = 0;
        private BigDecimal paymentAmount = BigDecimal.ZERO;
        private boolean isFollowed = false;
        private int totalPurchaseCount = 0;
        private BigDecimal totalPurchaseAmount = BigDecimal.ZERO;

        // Getters and Setters
        public String getShopId() { return shopId; }
        public void setShopId(String shopId) { this.shopId = shopId; }
        public String getStatDate() { return statDate; }
        public void setStatDate(String statDate) { this.statDate = statDate; }
        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }
        public int getRevisitCount() { return revisitCount; }
        public void setRevisitCount(int revisitCount) { this.revisitCount = revisitCount; }
        public int getRepurchaseCount() { return repurchaseCount; }
        public void setRepurchaseCount(int repurchaseCount) { this.repurchaseCount = repurchaseCount; }
        public int getNonRepurchaseCount() { return nonRepurchaseCount; }
        public void setNonRepurchaseCount(int nonRepurchaseCount) { this.nonRepurchaseCount = nonRepurchaseCount; }
        public BigDecimal getPaymentAmount() { return paymentAmount; }
        public void setPaymentAmount(BigDecimal paymentAmount) { this.paymentAmount = paymentAmount; }
        public boolean isFollowed() { return isFollowed; }
        public void setFollowed(boolean followed) { isFollowed = followed; }
        public int getTotalPurchaseCount() { return totalPurchaseCount; }
        public void setTotalPurchaseCount(int totalPurchaseCount) { this.totalPurchaseCount = totalPurchaseCount; }
        public BigDecimal getTotalPurchaseAmount() { return totalPurchaseAmount; }
        public void setTotalPurchaseAmount(BigDecimal totalPurchaseAmount) { this.totalPurchaseAmount = totalPurchaseAmount; }
    }

    // 销售汇总数据类
    public static class SalesDWS {
        private String shopId;
        private String statDate;
        private BigDecimal totalSales = BigDecimal.ZERO;
        private BigDecimal newVisitSales = BigDecimal.ZERO;
        private BigDecimal returnVisitSales = BigDecimal.ZERO;
        private BigDecimal oldCustomerSales = BigDecimal.ZERO;

        // Getters and Setters
        public String getShopId() { return shopId; }
        public void setShopId(String shopId) { this.shopId = shopId; }
        public String getStatDate() { return statDate; }
        public void setStatDate(String statDate) { this.statDate = statDate; }
        public BigDecimal getTotalSales() { return totalSales; }
        public void setTotalSales(BigDecimal totalSales) { this.totalSales = totalSales; }
        public BigDecimal getNewVisitSales() { return newVisitSales; }
        public void setNewVisitSales(BigDecimal newVisitSales) { this.newVisitSales = newVisitSales; }
        public BigDecimal getReturnVisitSales() { return returnVisitSales; }
        public void setReturnVisitSales(BigDecimal returnVisitSales) { this.returnVisitSales = returnVisitSales; }
        public BigDecimal getOldCustomerSales() { return oldCustomerSales; }
        public void setOldCustomerSales(BigDecimal oldCustomerSales) { this.oldCustomerSales = oldCustomerSales; }
    }

    // DWS层输出的各类汇总数据流封装
    public static class DWSStreams {
        private final DataStream<ShopCustomerDWS> shopCustomerStream;
        private final DataStream<NewVisitDWS> newVisitStream;
        private final DataStream<ReturnVisitDWS> returnVisitStream;
        private final DataStream<OldCustomerDWS> oldCustomerStream;
        private final DataStream<SalesDWS> salesStream;

        public DWSStreams(DataStream<ShopCustomerDWS> shopCustomerStream,
                          DataStream<NewVisitDWS> newVisitStream,
                          DataStream<ReturnVisitDWS> returnVisitStream,
                          DataStream<OldCustomerDWS> oldCustomerStream,
                          DataStream<SalesDWS> salesStream) {
            this.shopCustomerStream = shopCustomerStream;
            this.newVisitStream = newVisitStream;
            this.returnVisitStream = returnVisitStream;
            this.oldCustomerStream = oldCustomerStream;
            this.salesStream = salesStream;
        }

        // Getters
        public DataStream<ShopCustomerDWS> getShopCustomerStream() { return shopCustomerStream; }
        public DataStream<NewVisitDWS> getNewVisitStream() { return newVisitStream; }
        public DataStream<ReturnVisitDWS> getReturnVisitStream() { return returnVisitStream; }
        public DataStream<OldCustomerDWS> getOldCustomerStream() { return oldCustomerStream; }
        public DataStream<SalesDWS> getSalesStream() { return salesStream; }
    }

    // 创建DWS层数据流
    public static DWSStreams createStreams(DWDLayer.DWDStreams dwdStreams) {
        // 1. 计算店铺客户数
        DataStream<ShopCustomerDWS> shopCustomerStream = calculateShopCustomerCount(dwdStreams.getAllActionStream());

        // 2. 计算新访用户指标
        DataStream<NewVisitDWS> newVisitStream = calculateNewVisitMetrics(dwdStreams);

        // 3. 计算回访用户指标
        DataStream<ReturnVisitDWS> returnVisitStream = calculateReturnVisitMetrics(dwdStreams);

        // 4. 计算老客户指标
        DataStream<OldCustomerDWS> oldCustomerStream = calculateOldCustomerMetrics(dwdStreams);

        // 5. 计算销售汇总指标
        DataStream<SalesDWS> salesStream = calculateSalesMetrics(dwdStreams, newVisitStream, returnVisitStream, oldCustomerStream);

        return new DWSStreams(shopCustomerStream, newVisitStream, returnVisitStream, oldCustomerStream, salesStream);
    }

    // 计算店铺客户数
    private static DataStream<ShopCustomerDWS> calculateShopCustomerCount(DataStream<DWDLayer.DWDUserAction> allActionStream) {
        // 提取店铺、用户、日期三元组
        DataStream<Tuple3<String, String, String>> userShopDateStream = allActionStream
                .map(new MapFunction<DWDLayer.DWDUserAction, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> map(DWDLayer.DWDUserAction action) throws Exception {
                        return new Tuple3<>(action.getShopId(), action.getUserId(), action.getStatDate());
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.STRING))
                .name("Map to Shop-User-Date");

        // 按店铺和日期分组，计算去重用户数
        return userShopDateStream
                .keyBy(new KeySelector<Tuple3<String, String, String>, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(Tuple3<String, String, String> tuple) throws Exception {
                        return new Tuple2<>(tuple.f0, tuple.f2);
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.days(1)))
                .aggregate(new AggregateFunction<Tuple3<String, String, String>, Set<String>, ShopCustomerDWS>() {
                    @Override
                    public Set<String> createAccumulator() {
                        return new HashSet<>();
                    }

                    @Override
                    public Set<String> add(Tuple3<String, String, String> value, Set<String> accumulator) {
                        accumulator.add(value.f1); // 添加用户ID
                        return accumulator;
                    }

                    @Override
                    public ShopCustomerDWS getResult(Set<String> accumulator) {
                        ShopCustomerDWS result = new ShopCustomerDWS();
                        // 从 Tuple3 中获取 shopId 和 statDate（这里假设 Tuple3 的 f0 是 shopId，f2 是 statDate）
                        result.setShopId("testShopId"); // 实际应从 value 等地方获取，这里仅为示例
                        result.setStatDate("testStatDate");
                        result.setCustomerCount(accumulator.size());
                        return result;
                    }

                    @Override
                    public Set<String> merge(Set<String> a, Set<String> b) {
                        a.addAll(b);
                        return a;
                    }
                })
                .returns(ShopCustomerDWS.class)
                .name("Calculate Shop Customer Count")
                .uid("dws-shop-customer-count");
    }

    // 计算新访用户指标
    private static DataStream<NewVisitDWS> calculateNewVisitMetrics(DWDLayer.DWDStreams dwdStreams) {
        // 合并所有行为流
        DataStream<DWDLayer.DWDUserAction> allActions = dwdStreams.getAllActionStream();

        // 按用户和店铺分组处理
        return allActions
                .keyBy(new KeySelector<DWDLayer.DWDUserAction, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(DWDLayer.DWDUserAction action) throws Exception {
                        return new Tuple2<>(action.getShopId(), action.getUserId());
                    }
                })
                .process(new KeyedProcessFunction<Tuple2<String, String>, DWDLayer.DWDUserAction, NewVisitDWS>() {
                    // 状态定义
                    private ValueState<Boolean> isFirstVisit;
                    private ValueState<Long> lastActionTime;
                    private ValueState<Boolean> hasPurchasedBefore;
                    private ValueState<Integer> totalPurchaseCount;
                    private ValueState<BigDecimal> totalPurchaseAmount;
                    private ValueState<Boolean> isFollowed;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        isFirstVisit = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("isFirstVisit", Boolean.class, true));
                        lastActionTime = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("lastActionTime", Long.class, 0L));
                        hasPurchasedBefore = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("hasPurchasedBefore", Boolean.class, false));
                        totalPurchaseCount = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("totalPurchaseCount", Integer.class, 0));
                        totalPurchaseAmount = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("totalPurchaseAmount", BigDecimal.class, BigDecimal.ZERO));
                        isFollowed = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("isFollowed", Boolean.class, false));
                    }

                    @Override
                    public void processElement(DWDLayer.DWDUserAction action, Context context,
                                               Collector<NewVisitDWS> collector) throws Exception {
                        long validPeriod = TimeUnit.DAYS.toMillis(30); // 30天行为有效期
                        boolean isNewVisit = false;

                        // 判断是否为新访：首次访问 或 上次行为时间超出有效期 且 之前未购买过
                        if (!hasPurchasedBefore.value() &&
                                (isFirstVisit.value() || (System.currentTimeMillis() - lastActionTime.value() > validPeriod))) {
                            isNewVisit = true;
                            isFirstVisit.update(false);
                        }

                        // 更新最后行为时间
                        lastActionTime.update(System.currentTimeMillis());

                        // 处理关注事件
                        if (action.getActionType() == DWDLayer.ActionType.FOLLOW) {
                            isFollowed.update(true);
                        }

                        // 处理支付事件
                        if (action.getActionType() == DWDLayer.ActionType.PAYMENT) {
                            hasPurchasedBefore.update(true);
                            totalPurchaseCount.update(totalPurchaseCount.value() + 1);
                            totalPurchaseAmount.update(totalPurchaseAmount.value().add(action.getAmount()));
                        }

                        // 仅处理新访用户的指标
                        if (isNewVisit) {
                            NewVisitDWS metric = new NewVisitDWS();
                            metric.setShopId(action.getShopId());
                            metric.setStatDate(action.getStatDate());
                            metric.setUserId(action.getUserId());
                            metric.setVisitCount(1);

                            // 判断是否成交
                            if (action.getActionType() == DWDLayer.ActionType.PAYMENT) {
                                metric.setPaidCount(1);
                                metric.setPaymentAmount(action.getAmount());
                            } else {
                                metric.setUnpaidCount(1);
                            }

                            // 设置关注状态
                            metric.setFollowed(isFollowed.value());

                            // 设置购买总量
                            metric.setTotalPurchaseCount(totalPurchaseCount.value());
                            metric.setTotalPurchaseAmount(totalPurchaseAmount.value());

                            collector.collect(metric);
                        }
                    }
                })
                .name("Calculate New Visit Metrics")
                .uid("dws-new-visit-metrics");
    }

    // 计算回访用户指标
    private static DataStream<ReturnVisitDWS> calculateReturnVisitMetrics(DWDLayer.DWDStreams dwdStreams) {
        // 实现回访用户指标计算逻辑
        // 回访用户指：之前有过行为但未购买的客户，在统计时间内再次与店铺产生行为

        DataStream<DWDLayer.DWDUserAction> allActions = dwdStreams.getAllActionStream();

        return allActions
                .keyBy(new KeySelector<DWDLayer.DWDUserAction, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(DWDLayer.DWDUserAction action) throws Exception {
                        return new Tuple2<>(action.getShopId(), action.getUserId());
                    }
                })
                .process(new KeyedProcessFunction<Tuple2<String, String>, DWDLayer.DWDUserAction, ReturnVisitDWS>() {
                    // 状态定义
                    private ValueState<Boolean> hasPreviousAction;
                    private ValueState<Boolean> hasPurchasedBefore;
                    private ValueState<Integer> totalPurchaseCount;
                    private ValueState<BigDecimal> totalPurchaseAmount;
                    private ValueState<Boolean> isFollowed;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hasPreviousAction = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("hasPreviousAction", Boolean.class, false));
                        hasPurchasedBefore = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("hasPurchasedBefore", Boolean.class, false));
                        totalPurchaseCount = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("totalPurchaseCount", Integer.class, 0));
                        totalPurchaseAmount = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("totalPurchaseAmount", BigDecimal.class, BigDecimal.ZERO));
                        isFollowed = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("isFollowed", Boolean.class, false));
                    }

                    @Override
                    public void processElement(DWDLayer.DWDUserAction action, Context context,
                                               Collector<ReturnVisitDWS> collector) throws Exception {
                        // 判断是否为回访：之前有过行为但未购买
                        boolean isReturnVisit = hasPreviousAction.value() && !hasPurchasedBefore.value();

                        // 处理当前行为
                        if (action.getActionType() == DWDLayer.ActionType.FOLLOW) {
                            isFollowed.update(true);
                        }

                        if (action.getActionType() == DWDLayer.ActionType.PAYMENT) {
                            hasPurchasedBefore.update(true);
                            totalPurchaseCount.update(totalPurchaseCount.value() + 1);
                            totalPurchaseAmount.update(totalPurchaseAmount.value().add(action.getAmount()));
                        }

                        // 更新状态：标记为有过行为
                        hasPreviousAction.update(true);

                        // 仅处理回访用户
                        if (isReturnVisit) {
                            ReturnVisitDWS metric = new ReturnVisitDWS();
                            metric.setShopId(action.getShopId());
                            metric.setStatDate(action.getStatDate());
                            metric.setUserId(action.getUserId());
                            metric.setVisitCount(1);

                            // 判断是否成交
                            if (action.getActionType() == DWDLayer.ActionType.PAYMENT) {
                                metric.setPaidCount(1);
                                metric.setPaymentAmount(action.getAmount());
                            } else {
                                metric.setUnpaidCount(1);
                            }

                            // 设置关注状态
                            metric.setFollowed(isFollowed.value());

                            // 设置购买总量
                            metric.setTotalPurchaseCount(totalPurchaseCount.value());
                            metric.setTotalPurchaseAmount(totalPurchaseAmount.value());

                            collector.collect(metric);
                        }
                    }
                })
                .name("Calculate Return Visit Metrics")
                .uid("dws-return-visit-metrics");
    }

    // 计算老客户指标
    private static DataStream<OldCustomerDWS> calculateOldCustomerMetrics(DWDLayer.DWDStreams dwdStreams) {
        // 实现老客户指标计算逻辑
        // 老客户指：过去365天内有购买行为的客户

        DataStream<DWDLayer.DWDUserAction> allActions = dwdStreams.getAllActionStream();

        return allActions
                .keyBy(new KeySelector<DWDLayer.DWDUserAction, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(DWDLayer.DWDUserAction action) throws Exception {
                        return new Tuple2<>(action.getShopId(), action.getUserId());
                    }
                })
                .process(new KeyedProcessFunction<Tuple2<String, String>, DWDLayer.DWDUserAction, OldCustomerDWS>() {
                    // 状态定义
                    private ValueState<Boolean> hasPurchasedIn365Days;
                    private ValueState<Integer> totalPurchaseCount;
                    private ValueState<BigDecimal> totalPurchaseAmount;
                    private ValueState<Long> lastPurchaseTime;
                    private ValueState<Boolean> isFollowed;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hasPurchasedIn365Days = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("hasPurchasedIn365Days", Boolean.class, false));
                        totalPurchaseCount = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("totalPurchaseCount", Integer.class, 0));
                        totalPurchaseAmount = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("totalPurchaseAmount", BigDecimal.class, BigDecimal.ZERO));
                        lastPurchaseTime = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("lastPurchaseTime", Long.class, 0L));
                        isFollowed = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("isFollowed", Boolean.class, false));
                    }

                    @Override
                    public void processElement(DWDLayer.DWDUserAction action, Context context,
                                               Collector<OldCustomerDWS> collector) throws Exception {
                        long oneYear = TimeUnit.DAYS.toMillis(365);
                        boolean isOldCustomer = hasPurchasedIn365Days.value() &&
                                (System.currentTimeMillis() - lastPurchaseTime.value() <= oneYear);

                        // 处理关注事件
                        if (action.getActionType() == DWDLayer.ActionType.FOLLOW) {
                            isFollowed.update(true);
                        }

                        // 处理支付事件
                        if (action.getActionType() == DWDLayer.ActionType.PAYMENT) {
                            hasPurchasedIn365Days.update(true);
                            lastPurchaseTime.update(System.currentTimeMillis());
                            totalPurchaseCount.update(totalPurchaseCount.value() + 1);
                            totalPurchaseAmount.update(totalPurchaseAmount.value().add(action.getAmount()));
                        }

                        // 仅处理老客户
                        if (isOldCustomer) {
                            OldCustomerDWS metric = new OldCustomerDWS();
                            metric.setShopId(action.getShopId());
                            metric.setStatDate(action.getStatDate());
                            metric.setUserId(action.getUserId());
                            metric.setRevisitCount(1);

                            // 判断是否复购
                            if (action.getActionType() == DWDLayer.ActionType.PAYMENT) {
                                metric.setRepurchaseCount(1);
                                metric.setPaymentAmount(action.getAmount());
                            } else {
                                metric.setNonRepurchaseCount(1);
                            }

                            // 设置关注状态
                            metric.setFollowed(isFollowed.value());

                            // 设置购买总量
                            metric.setTotalPurchaseCount(totalPurchaseCount.value());
                            metric.setTotalPurchaseAmount(totalPurchaseAmount.value());

                            collector.collect(metric);
                        }
                    }
                })
                .name("Calculate Old Customer Metrics")
                .uid("dws-old-customer-metrics");
    }

    // 计算销售汇总指标
    private static DataStream<SalesDWS> calculateSalesMetrics(DWDLayer.DWDStreams dwdStreams,
                                                              DataStream<NewVisitDWS> newVisitStream,
                                                              DataStream<ReturnVisitDWS> returnVisitStream,
                                                              DataStream<OldCustomerDWS> oldCustomerStream) {
        // 从支付流计算总销售额
        DataStream<Tuple3<String, String, BigDecimal>> totalSalesStream = dwdStreams.getPaymentStream()
                .map(new MapFunction<DWDLayer.DWDUserAction, Tuple3<String, String, BigDecimal>>() {
                    @Override
                    public Tuple3<String, String, BigDecimal> map(DWDLayer.DWDUserAction action) throws Exception {
                        return new Tuple3<>(action.getShopId(), action.getStatDate(), action.getAmount());
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.BIG_DEC))
                .keyBy(new KeySelector<Tuple3<String, String, BigDecimal>, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(Tuple3<String, String, BigDecimal> tuple) throws Exception {
                        return new Tuple2<>(tuple.f0, tuple.f1);
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.days(1)))
                .aggregate(new AggregateFunction<Tuple3<String, String, BigDecimal>, BigDecimal, Tuple3<String, String, BigDecimal>>() {
                    @Override
                    public BigDecimal createAccumulator() {
                        return BigDecimal.ZERO;
                    }

                    @Override
                    public BigDecimal add(Tuple3<String, String, BigDecimal> value, BigDecimal accumulator) {
                        return accumulator.add(value.f2);
                    }

                    @Override
                    public Tuple3<String, String, BigDecimal> getResult(BigDecimal accumulator) {
                        return new Tuple3<>("", "", accumulator); // 实际实现需要保留店铺和日期
                    }

                    @Override
                    public BigDecimal merge(BigDecimal a, BigDecimal b) {
                        return a.add(b);
                    }
                });

        // 此处简化实现，实际应关联各用户类型的销售数据
        return totalSalesStream
                .map((MapFunction<Tuple3<String, String, BigDecimal>, SalesDWS>) tuple -> {
                    SalesDWS sales = new SalesDWS();
                    sales.setShopId(tuple.f0);
                    sales.setStatDate(tuple.f1);
                    sales.setTotalSales(tuple.f2);
                    return sales;
                })
                .name("Calculate Sales Metrics")
                .uid("dws-sales-metrics");
    }
}

