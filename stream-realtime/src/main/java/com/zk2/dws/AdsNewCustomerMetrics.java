package com.zk2.dws;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 新访客户指标计算ADS层
 */
public class AdsNewCustomerMetrics {

    // 输入Kafka主题（DWD层店铺行为数据）
    private static final String INPUT_TOPIC = ConfigUtils.getString("kafka.dwd.shop.behavior.topic");
    // 输出Kafka主题（ADS层新访客户指标）
    private static final String OUTPUT_TOPIC = ConfigUtils.getString("kafka.dws.shop.metrics.topic");

    public static void main(String[] args) throws Exception {
        // 1. 初始化执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取DWD层数据
        DataStream<JSONObject> dwdStream = env.fromSource(
                        KafkaUtils.buildKafkaSource(
                                ConfigUtils.getString("kafka.bootstrap.servers"),
                                INPUT_TOPIC,
                                "ads_new_customer_consumer",
                                org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()
                        ),
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((SerializableTimestampAssigner<String>) (element, recordTimestamp) ->
                                        JSONObject.parseObject(element).getLong("ts")
                                ),
                        "new_customer_source"
                ).map(JSONObject::parseObject)
                .name("dwd_data_parse");
//        dwdStream.print();
        // 3. 识别新访用户并标记行为类型
        SingleOutputStreamOperator<Tuple3<String, String, JSONObject>> userBehaviorStream = dwdStream
                .keyBy((KeySelector<JSONObject, String>) json -> json.getString("uid"))
                .process(new UserVisitTypeProcessFunction())
                .name("identify_new_visitor");
        userBehaviorStream.print();
        // 4. 按天聚合计算各项指标
//        SingleOutputStreamOperator<String> metricsStream = userBehaviorStream
//                .keyBy(tuple -> tuple.f0)  // 按日期分组
//                .window(TumblingProcessingTimeWindows.of(Time.milliseconds(1)))
//                .aggregate(new CustomerMetricsAggregate(), new CustomerMetricsWindowProcess())
//                .name("calculate_new_customer_metrics");
//        metricsStream.print();
//        // 5. 输出到Kafka
//        metricsStream.sinkTo(KafkaUtils.buildKafkaSink(
//                ConfigUtils.getString("kafka.bootstrap.servers"),
//                OUTPUT_TOPIC
//        )).name("new_customer_metrics_sink");

        env.execute("ADS_New_Customer_Metrics_Process");
    }

    /**
     * 识别用户是否为新访用户，并标记行为类型
     */
    public static class UserVisitTypeProcessFunction extends KeyedProcessFunction<String, JSONObject, Tuple3<String, String, JSONObject>> {
        // 存储用户首次访问时间 (yyyy-MM-dd)
        private ValueState<String> firstVisitDateState;
        // 存储用户当日是否已支付
        private ValueState<Boolean> hasPaidTodayState;
        // 存储用户总交易金额
        private ValueState<Double> totalPaymentState;
        // 存储用户交易次数
        private ValueState<Integer> transactionCountState;
        // 存储用户是否关注店铺
        private ValueState<Boolean> isFollowState;

        @Override
        public void open(Configuration parameters) throws Exception {
            firstVisitDateState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("firstVisitDate", String.class));
            hasPaidTodayState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("hasPaidToday", Boolean.class, false));
            totalPaymentState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("totalPayment", Double.class, 0.0));
            transactionCountState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("transactionCount", Integer.class, 0));
            isFollowState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("isFollow", Boolean.class, false));
        }

        @Override
        public void processElement(JSONObject value, Context ctx, Collector<Tuple3<String, String, JSONObject>> out) throws Exception {
            String uid = value.getString("uid");
            long ts = value.getLong("ts");
            String behaviorType = value.getString("behavior_type");
            String currentDate = TimeUtils.formatDate(ts);  // 自定义时间工具类，将时间戳转为yyyy-MM-dd格式

            // 获取用户首次访问日期
            String firstVisitDate = firstVisitDateState.value();
            boolean isNewVisitor = false;

            // 首次访问
            if (firstVisitDate == null) {
                firstVisitDate = currentDate;
                firstVisitDateState.update(firstVisitDate);
                isNewVisitor = true;
            } else {
                // 判断是否超出行为有效期（假设行为有效期为30天，可配置）
                long daysSinceLastVisit = TimeUtils.daysBetween(firstVisitDate, currentDate);
                if (daysSinceLastVisit > ConfigUtils.getInt("customer.behavior.valid.days", 30)) {
                    firstVisitDateState.update(currentDate);
                    isNewVisitor = true;
                }
            }

            // 处理支付行为
            if ("payment".equals(behaviorType)) {
                // 简单起见，假设支付金额从item中解析，实际应根据业务调整
                double payment = parsePaymentAmount(value.getString("item"));
                totalPaymentState.update(totalPaymentState.value() + payment);
                transactionCountState.update(transactionCountState.value() + 1);

                // 如果是新访用户，标记当日已支付
                if (isNewVisitor && currentDate.equals(firstVisitDate)) {
                    hasPaidTodayState.update(true);
                }
            }

            // 处理关注行为（假设behavior_type为"favor_add"代表关注）
            if ("favor_add".equals(behaviorType)) {
                isFollowState.update(true);
            }

            // 输出：日期、用户ID、行为数据（附加新访标记）
            value.put("is_new_visitor", isNewVisitor);
            value.put("first_visit_date", firstVisitDate);
            out.collect(Tuple3.of(currentDate, uid, value));
        }

        // 解析支付金额（示例方法，实际应根据业务逻辑实现）
        private double parsePaymentAmount(String item) {
            // 这里只是简单示例，实际业务中应有明确的金额字段
            return 100.0;  // 默认值，实际需根据数据格式解析
        }
    }

    /**
     * 聚合函数：计算各项指标的中间结果
     */
    public static class CustomerMetricsAggregate implements AggregateFunction<
            Tuple3<String, String, JSONObject>,
            CustomerMetrics,
            CustomerMetrics> {

        @Override
        public CustomerMetrics createAccumulator() {
            return new CustomerMetrics();
        }

        @Override
        public CustomerMetrics add(Tuple3<String, String, JSONObject> value, CustomerMetrics accumulator) {
            String uid = value.f1;
            JSONObject data = value.f2;
            boolean isNewVisitor = data.getBoolean("is_new_visitor");

            // 店铺客户数（去重）
            accumulator.shopCustomerSet.add(uid);

            // 处理新访用户指标
            if (isNewVisitor) {
                // 新访客户数（去重）
                accumulator.newVisitorSet.add(uid);

                // 新访成交客户
                if ("payment".equals(data.getString("behavior_type"))) {
                    accumulator.newVisitorPaidSet.add(uid);
                    // 累加新访支付金额
                    accumulator.newVisitorPaymentAmount += parsePaymentAmount(data.getString("item"));
                }

                // 新访粉丝数
                if (data.getBoolean("is_follow") != null && data.getBoolean("is_follow")) {
                    accumulator.newVisitorFollowSet.add(uid);
                }

                // 新访会员数（交易额超5000或交易次数超3次）
                Double totalPayment = data.getDouble("total_payment");
                Integer transCount = data.getInteger("transaction_count");
                if ((totalPayment != null && totalPayment > 5000) || (transCount != null && transCount > 3)) {
                    accumulator.newVisitorMemberSet.add(uid);
                }
            }

            // 累加总销售额（用于计算新访支付金额占比）
            if ("payment".equals(data.getString("behavior_type"))) {
                accumulator.totalPaymentAmount += parsePaymentAmount(data.getString("item"));
            }

            return accumulator;
        }

        @Override
        public CustomerMetrics getResult(CustomerMetrics accumulator) {
            return accumulator;
        }

        @Override
        public CustomerMetrics merge(CustomerMetrics a, CustomerMetrics b) {
            a.shopCustomerSet.addAll(b.shopCustomerSet);
            a.newVisitorSet.addAll(b.newVisitorSet);
            a.newVisitorPaidSet.addAll(b.newVisitorPaidSet);
            a.newVisitorPaymentAmount += b.newVisitorPaymentAmount;
            a.totalPaymentAmount += b.totalPaymentAmount;
            a.newVisitorFollowSet.addAll(b.newVisitorFollowSet);
            a.newVisitorMemberSet.addAll(b.newVisitorMemberSet);
            return a;
        }

        private double parsePaymentAmount(String item) {
            return 100.0;  // 同上文，实际需根据业务逻辑实现
        }
    }

    /**
     * 窗口处理函数：计算最终指标并格式化输出
     */
    public static class CustomerMetricsWindowProcess extends ProcessWindowFunction<
            CustomerMetrics,
            String,
            String,
            TimeWindow> {

        @Override
        public void process(String date, Context context, Iterable<CustomerMetrics> elements, Collector<String> out) throws Exception {
            CustomerMetrics metrics = elements.iterator().next();

            // 计算各项指标
            int shopCustomerCount = metrics.shopCustomerSet.size();
            int newVisitorCount = metrics.newVisitorSet.size();
            int newVisitorPaidCount = metrics.newVisitorPaidSet.size();
            int newVisitorUnpaidCount = newVisitorCount - newVisitorPaidCount;

            // 新访支付转化率
            double newVisitorConversionRate = newVisitorCount > 0 ?
                    (double) newVisitorPaidCount / newVisitorCount : 0;

            // 新访支付金额占比
            double newVisitorPaymentRatio = metrics.totalPaymentAmount > 0 ?
                    metrics.newVisitorPaymentAmount / metrics.totalPaymentAmount : 0;

            // 新访客单价
            double newVisitorAvgPrice = newVisitorPaidCount > 0 ?
                    metrics.newVisitorPaymentAmount / newVisitorPaidCount : 0;

            // 新访粉丝数
            int newVisitorFollowCount = metrics.newVisitorFollowSet.size();

            // 新访会员数
            int newVisitorMemberCount = metrics.newVisitorMemberSet.size();

            // 构建结果JSON
            JSONObject result = new JSONObject();
            result.put("stat_date", date);
            result.put("shop_customer_count", shopCustomerCount);
            result.put("new_visitor_count", newVisitorCount);
            result.put("new_visitor_paid_count", newVisitorPaidCount);
            result.put("new_visitor_unpaid_count", newVisitorUnpaidCount);
            result.put("new_visitor_conversion_rate", String.format("%.2f%%", newVisitorConversionRate * 100));
            result.put("new_visitor_payment_ratio", String.format("%.2f%%", newVisitorPaymentRatio * 100));
            result.put("new_visitor_avg_price", String.format("%.2f", newVisitorAvgPrice));
            result.put("new_visitor_follow_count", newVisitorFollowCount);
            result.put("new_visitor_member_count", newVisitorMemberCount);
            result.put("calc_time", System.currentTimeMillis());

            out.collect(result.toJSONString());
        }
    }

    /**
     * 指标聚合中间状态类
     */
    public static class CustomerMetrics {
        // 店铺客户集合（去重）
        public final Set<String> shopCustomerSet = new HashSet<>();
        // 新访客户集合（去重）
        public final Set<String> newVisitorSet = new HashSet<>();
        // 新访成交客户集合（去重）
        public final Set<String> newVisitorPaidSet = new HashSet<>();
        // 新访支付总金额
        public double newVisitorPaymentAmount = 0.0;
        // 店铺总支付金额
        public double totalPaymentAmount = 0.0;
        // 新访粉丝集合（去重）
        public final Set<String> newVisitorFollowSet = new HashSet<>();
        // 新访会员集合（去重）
        public final Set<String> newVisitorMemberSet = new HashSet<>();
    }

    /**
     * 时间工具类（简化实现）
     */
    public static class TimeUtils {
        // 将时间戳转换为yyyy-MM-dd格式
        public static String formatDate(long timestamp) {
            // 实际实现需使用SimpleDateFormat或Java 8+的DateTimeFormatter
            return new SimpleDateFormat("yyyy-MM-dd").format(new Date(timestamp));
        }

        // 计算两个日期之间的天数差
        public static long daysBetween(String date1, String date2) {
            try {
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
                Date d1 = sdf.parse(date1);
                Date d2 = sdf.parse(date2);
                long diff = d2.getTime() - d1.getTime();
                return diff / (1000 * 60 * 60 * 24);
            } catch (Exception e) {
                return 0;
            }
        }
    }
}