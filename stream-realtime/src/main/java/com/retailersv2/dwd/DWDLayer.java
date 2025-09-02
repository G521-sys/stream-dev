package com.retailersv2.dwd;

import com.retailersv2.ods.ODSLayer;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 数据明细层(DWD)
 * 负责从ODS层接入原始数据，清洗/标准化后输出多类行为流
 */
public class DWDLayer {
    // 日期格式化器（统计日期：yyyy-MM-dd）
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
    // 侧输出流标签：用于输出无效/异常数据
    private static final OutputTag<String> INVALID_DATA_TAG = new OutputTag<String>("invalid-data") {};

    // 标准化行为类型枚举（统一行为分类）
    public enum ActionType {
        VISIT,       // 访问行为
        INTERACTION, // 有效互动行为（如短视频<3秒且点赞）
        PAYMENT,     // 支付行为
        FOLLOW       // 关注行为
    }

    // 标准化后的用户行为数据类（DWD层核心数据结构）
    public static class DWDUserAction {
        private String actionId;    // 行为唯一ID（继承自ODS）
        private String userId;      // 用户ID
        private String shopId;      // 店铺ID
        private long timestamp;     // 行为时间戳（毫秒级）
        private String statDate;    // 统计日期（yyyy-MM-dd，新增字段）
        private ActionType actionType; // 标准化行为类型
        private BigDecimal amount = BigDecimal.ZERO; // 支付金额（仅支付行为有值）
        private String relatedId;   // 关联ID（互动=内容ID，支付=订单ID）

        // Getters and Setters
        public String getActionId() { return actionId; }
        public void setActionId(String actionId) { this.actionId = actionId; }
        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }
        public String getShopId() { return shopId; }
        public void setShopId(String shopId) { this.shopId = shopId; }
        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
        public String getStatDate() { return statDate; }
        public void setStatDate(String statDate) { this.statDate = statDate; }
        public ActionType getActionType() { return actionType; }
        public void setActionType(ActionType actionType) { this.actionType = actionType; }
        public BigDecimal getAmount() { return amount; }
        public void setAmount(BigDecimal amount) { this.amount = amount; }
        public String getRelatedId() { return relatedId; }
        public void setRelatedId(String relatedId) { this.relatedId = relatedId; }

        // 重写toString：清晰输出核心字段（隐藏冗余字段如actionId，按需调整）
        @Override
        public String toString() {
            return "DWDUserAction{" +
                    "userId='" + userId + '\'' +
                    ", shopId='" + shopId + '\'' +
                    ", statDate='" + statDate + '\'' +
                    ", actionType=" + actionType +
                    ", amount=" + amount +
                    ", relatedId='" + relatedId + '\'' +
                    '}';
        }
    }

    // DWD层输出流封装类（统一管理四类有效流+一类无效流）
    public static class DWDStreams {
        private final DataStream<DWDUserAction> visitStream;         // 访问行为流
        private final DataStream<DWDUserAction> interactionStream;   // 有效互动流
        private final DataStream<DWDUserAction> paymentStream;       // 支付行为流
        private final DataStream<DWDUserAction> followStream;        // 关注行为流
        private final DataStream<DWDUserAction> allActionStream;     // 全量有效行为流
        private final DataStream<String> invalidDataStream;          // 无效/异常数据流

        // 构造器：初始化所有流
        public DWDStreams(DataStream<DWDUserAction> visitStream,
                          DataStream<DWDUserAction> interactionStream,
                          DataStream<DWDUserAction> paymentStream,
                          DataStream<DWDUserAction> followStream,
                          DataStream<DWDUserAction> allActionStream,
                          DataStream<String> invalidDataStream) {
            this.visitStream = visitStream;
            this.interactionStream = interactionStream;
            this.paymentStream = paymentStream;
            this.followStream = followStream;
            this.allActionStream = allActionStream;
            this.invalidDataStream = invalidDataStream;
        }

        // Getters：提供外部访问流的接口
        public DataStream<DWDUserAction> getVisitStream() { return visitStream; }
        public DataStream<DWDUserAction> getInteractionStream() { return interactionStream; }
        public DataStream<DWDUserAction> getPaymentStream() { return paymentStream; }
        public DataStream<DWDUserAction> getFollowStream() { return followStream; }
        public DataStream<DWDUserAction> getAllActionStream() { return allActionStream; }
        public DataStream<String> getInvalidDataStream() { return invalidDataStream; }
    }

    /**
     * 核心方法：创建DWD层数据流（从ODS接入→清洗→标准化→拆分流）
     * @param odsStream ODS层原始用户行为流
     * @return DWDStreams 封装后的所有输出流
     */
    public static DWDStreams createStreams(DataStream<ODSLayer.ODSUserAction> odsStream) {
        // 1. 数据清洗与标准化：核心处理逻辑（过滤无效数据+统一格式）
        SingleOutputStreamOperator<DWDUserAction> normalizedStream = odsStream
                .process(new ProcessFunction<ODSLayer.ODSUserAction, DWDUserAction>() {
                    @Override
                    public void processElement(ODSLayer.ODSUserAction rawAction, Context ctx,
                                               Collector<DWDUserAction> out) throws Exception {
                        try {
                            // 第一步：基础字段校验（过滤缺失核心字段的异常数据）
                            if (rawAction.getUserId() == null || rawAction.getShopId() == null ||
                                    rawAction.getTimestamp() <= 0 || rawAction.getActionType() == null) {
                                ctx.output(INVALID_DATA_TAG, "【无效数据-核心字段缺失】: " + rawAction);
                                return;
                            }

                            // 第二步：初始化标准化对象（继承ODS核心字段，新增标准化字段）
                            DWDUserAction dwdAction = new DWDUserAction();
                            dwdAction.setActionId(rawAction.getActionId());
                            dwdAction.setUserId(rawAction.getUserId());
                            dwdAction.setShopId(rawAction.getShopId());
                            dwdAction.setTimestamp(rawAction.getTimestamp());
                            dwdAction.setStatDate(DATE_FORMAT.format(new Date(rawAction.getTimestamp()))); // 新增统计日期

                            // 第三步：按ODS行为类型分类处理（核心标准化逻辑）
                            switch (rawAction.getActionType()) {
                                // 处理“访问”行为：直接标准化，无额外校验
                                case "visit":
                                    dwdAction.setActionType(ActionType.VISIT);
                                    out.collect(dwdAction);
                                    break;

                                // 处理“互动”行为：仅保留“短视频<3秒且点赞”的有效互动
                                case "interact":
                                    // 校验元数据完整性（必须包含duration/like/contentId）
                                    if (!rawAction.getMetadata().containsKey("duration") ||
                                            !rawAction.getMetadata().containsKey("like") ||
                                            !rawAction.getMetadata().containsKey("contentId")) {
                                        ctx.output(INVALID_DATA_TAG, "【无效互动-元数据缺失】: " + rawAction);
                                        break;
                                    }
                                    // 校验互动有效性（时长<3秒且点赞）
                                    int duration = (int) rawAction.getMetadata().get("duration");
                                    boolean isLike = (boolean) rawAction.getMetadata().get("like");
                                    if (duration < 3 && isLike) {
                                        dwdAction.setActionType(ActionType.INTERACTION);
                                        dwdAction.setRelatedId((String) rawAction.getMetadata().get("contentId")); // 关联内容ID
                                        out.collect(dwdAction);
                                    } else {
                                        ctx.output(INVALID_DATA_TAG, "【无效互动-未满足条件】: 时长=" + duration + "秒, 点赞=" + isLike + ", 原始数据=" + rawAction);
                                    }
                                    break;

                                // 处理“支付”行为：校验金额和订单ID，标准化金额字段
                                case "purchase":
                                    if (!rawAction.getMetadata().containsKey("amount") ||
                                            !rawAction.getMetadata().containsKey("orderId")) {
                                        ctx.output(INVALID_DATA_TAG, "【无效支付-元数据缺失】: " + rawAction);
                                        break;
                                    }
                                    dwdAction.setActionType(ActionType.PAYMENT);
                                    dwdAction.setAmount((BigDecimal) rawAction.getMetadata().get("amount")); // 标准化支付金额
                                    dwdAction.setRelatedId((String) rawAction.getMetadata().get("orderId")); // 关联订单ID
                                    out.collect(dwdAction);
                                    break;

                                // 处理“关注”行为：直接标准化
                                case "follow":
                                    dwdAction.setActionType(ActionType.FOLLOW);
                                    out.collect(dwdAction);
                                    break;

                                // 未知行为类型：过滤并标记
                                default:
                                    ctx.output(INVALID_DATA_TAG, "【无效数据-未知行为类型】: 类型=" + rawAction.getActionType() + ", 原始数据=" + rawAction);
                            }
                        } catch (Exception e) {
                            // 捕获异常数据（如类型转换错误）
                            ctx.output(INVALID_DATA_TAG, "【异常数据-处理失败】: 错误信息=" + e.getMessage() + ", 原始数据=" + rawAction);
                        }
                    }
                })
                .name("DWD-数据清洗与标准化") // 算子名称（便于Flink UI监控）
                .uid("dwd-data-normalization"); // 算子唯一ID（便于状态恢复）

        // 2. 拆分有效行为流：按标准化后的ActionType过滤
        DataStream<DWDUserAction> visitStream = normalizedStream
                .filter((FilterFunction<DWDUserAction>) action -> action.getActionType() == ActionType.VISIT)
                .name("DWD-访问行为流")
                .uid("dwd-visit-stream");

        DataStream<DWDUserAction> interactionStream = normalizedStream
                .filter((FilterFunction<DWDUserAction>) action -> action.getActionType() == ActionType.INTERACTION)
                .name("DWD-有效互动流")
                .uid("dwd-interaction-stream");

        DataStream<DWDUserAction> paymentStream = normalizedStream
                .filter((FilterFunction<DWDUserAction>) action -> action.getActionType() == ActionType.PAYMENT)
                .name("DWD-支付行为流")
                .uid("dwd-payment-stream");

        DataStream<DWDUserAction> followStream = normalizedStream
                .filter((FilterFunction<DWDUserAction>) action -> action.getActionType() == ActionType.FOLLOW)
                .name("DWD-关注行为流")
                .uid("dwd-follow-stream");

        // 3. 提取无效数据流（从侧输出流获取）
        DataStream<String> invalidDataStream = normalizedStream.getSideOutput(INVALID_DATA_TAG);

        // 4. 封装所有流并返回
        return new DWDStreams(
                visitStream,
                interactionStream,
                paymentStream,
                followStream,
                normalizedStream, // allActionStream：全量有效行为流
                invalidDataStream
        );
    }

    /**
     * 主方法：启动DWD层任务（接入ODS→处理→输出所有流）
     */
    public static void main(String[] args) throws Exception {
        // 1. 创建Flink执行环境（本地模式，并行度1确保数据顺序）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 第一步：从ODS层接入原始数据
        DataStream<ODSLayer.ODSUserAction> odsStream = ODSLayer.createStream(env);

        // 3. 第二步：执行DWD层处理逻辑，获取所有输出流
        DWDStreams dwdStreams = createStreams(odsStream);

        // 4. 第三步：输出所有流到控制台（按流分类打印，便于区分）
        // 4.1 输出有效行为流
        dwdStreams.getVisitStream().print("✅ DWD-访问流: ");
        dwdStreams.getInteractionStream().print("✅ DWD-有效互动流: ");
        dwdStreams.getPaymentStream().print("✅ DWD-支付流: ");
        dwdStreams.getFollowStream().print("✅ DWD-关注流: ");
        // 4.2 输出无效/异常流（用⚠️标记，便于排查问题）
        dwdStreams.getInvalidDataStream().print("⚠️ DWD-无效流: ");

        // 5. 启动Flink任务
        env.execute("DWDLayer-数据清洗与标准化任务");
    }
}
    