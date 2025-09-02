package com.retailersv2.ods;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * 操作数据存储层(ODS)
 * 负责接入、生成原始用户行为数据，并输出打印
 */
public class ODSLayer {

    // 生成用户行为数据流，并添加输出逻辑
    public static void main(String[] args) throws Exception {
        // 1. 创建Flink执行环境（本地模式，适用于测试）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度设置为1（确保数据按生成顺序输出，便于观察）
        env.setParallelism(1);

        // 2. 生成原始用户行为数据流
        DataStream<ODSUserAction> odsUserActionStream = createStream(env);

        // 3. 输出数据：方式1 - 直接打印到控制台（简洁，适用于测试）
        odsUserActionStream.print("ODS原始数据输出: ");

        // 3. 输出数据：方式2 - 自定义Sink（可扩展到文件/日志系统，此处示例打印）
        // odsUserActionStream.addSink(new CustomPrintSink()).name("ODS-Data-Print-Sink");

        // 4. 执行Flink任务
        env.execute("ODSLayer-Data-Generation-And-Print");
    }

    // 生成用户行为数据流（原有方法，未修改）
    public static DataStream<ODSUserAction> createStream(StreamExecutionEnvironment env) {
        return env.addSource(new UserActionSource())
                .name("ODS - User Action Source")
                .uid("ods-user-action-source");
    }

    // 原始用户行为数据类（原有结构，未修改）
    public static class ODSUserAction {
        private String actionId;
        private String userId;
        private String shopId;
        private long timestamp;
        private String actionType; // visit, interact, purchase, follow
        private Map<String, Object> metadata;

        public ODSUserAction() {}

        public ODSUserAction(String actionId, String userId, String shopId, long timestamp,
                             String actionType, Map<String, Object> metadata) {
            this.actionId = actionId;
            this.userId = userId;
            this.shopId = shopId;
            this.timestamp = timestamp;
            this.actionType = actionType;
            this.metadata = metadata;
        }

        // Getters and Setters
        public String getActionId() { return actionId; }
        public void setActionId(String actionId) { this.actionId = actionId; }
        public String getUserId() { return userId; }
        public void setUserId(String userId) { this.userId = userId; }
        public String getShopId() { return shopId; }
        public void setShopId(String shopId) { this.shopId = shopId; }
        public long getTimestamp() { return timestamp; }
        public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
        public String getActionType() { return actionType; }
        public void setActionType(String actionType) { this.actionType = actionType; }
        public Map<String, Object> getMetadata() { return metadata; }
        public void setMetadata(Map<String, Object> metadata) { this.metadata = metadata; }

        // 重写toString，确保输出格式清晰
        @Override
        public String toString() {
            return "ODSUserAction{" +
                    "actionId='" + actionId + '\'' +
                    ", userId='" + userId + '\'' +
                    ", shopId='" + shopId + '\'' +
                    ", timestamp=" + timestamp +
                    ", actionType='" + actionType + '\'' +
                    ", metadata=" + metadata +
                    '}';
        }
    }

    // 模拟用户行为数据源（原有逻辑，未修改）
    public static class UserActionSource implements SourceFunction<ODSUserAction> {
        private volatile boolean running = true;
        private Random random = new Random();
        // 预设10个用户ID，模拟固定用户群体
        private String[] userIds = {"user1", "user2", "user3", "user4", "user5", "user6", "user7", "user8", "user9", "user10"};
        // 预设3个店铺ID，模拟目标店铺
        private String[] shopIds = {"shop1", "shop2", "shop3"};
        // 4类核心行为类型
        private String[] actionTypes = {"visit", "interact", "purchase", "follow"};

        @Override
        public void run(SourceContext<ODSUserAction> ctx) throws Exception {
            while (running) {
                // 生成唯一行为ID
                String actionId = UUID.randomUUID().toString().replace("-", "");
                // 随机选择用户、店铺、行为类型
                String userId = userIds[random.nextInt(userIds.length)];
                String shopId = shopIds[random.nextInt(shopIds.length)];
                long timestamp = System.currentTimeMillis(); // 当前时间戳（毫秒级）
                String actionType = actionTypes[random.nextInt(actionTypes.length)];

                // 根据行为类型生成差异化元数据
                Map<String, Object> metadata = new HashMap<>();
                if (actionType.equals("interact")) { // 互动行为：含时长、是否点赞、内容ID
                    metadata.put("duration", random.nextInt(5)); // 0-4秒（模拟短视频观看时长）
                    metadata.put("like", random.nextBoolean()); // 随机是否点赞
                    metadata.put("contentId", "content_" + random.nextInt(100)); // 互动内容ID（如短视频ID）
                } else if (actionType.equals("purchase")) { // 支付行为：含金额、订单ID
                    metadata.put("amount", new BigDecimal(random.nextInt(10000) + 100)); // 100-10000元随机金额
                    metadata.put("orderId", "order_" + random.nextInt(10000)); // 唯一订单ID
                }
                // visit（访问）和follow（关注）行为无额外元数据，metadata为空

                // 构造原始行为数据对象并发送
                ODSUserAction action = new ODSUserAction(actionId, userId, shopId, timestamp, actionType, metadata);
                ctx.collect(action);

                // 控制数据生成速度：100-1000毫秒生成一条（模拟真实场景的稀疏行为）
                Thread.sleep(random.nextInt(900) + 100);
            }
        }

        @Override
        public void cancel() {
            running = false; // 任务取消时停止数据生成
        }
    }

    // 自定义Sink（可选）：比print()更灵活，可扩展到文件/数据库
    public static class CustomPrintSink extends RichSinkFunction<ODSUserAction> {
        @Override
        public void invoke(ODSUserAction value, Context context) throws Exception {
            // 自定义输出格式，例如增加时间戳的格式化显示
            String formatTime = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(value.getTimestamp());
            System.out.println("ODS自定义输出 [" + formatTime + "]: " + value);
        }
    }
}
    