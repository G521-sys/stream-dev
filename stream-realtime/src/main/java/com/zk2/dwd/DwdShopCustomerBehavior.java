package com.zk2.dwd;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class DwdShopCustomerBehavior {
    // 输入Kafka主题（原始行为日志）
    private static final String INPUT_TOPIC = ConfigUtils.getString("REALTIME.KAFKA.LOG.TOPIC");
    // 输出Kafka主题（清洗后的店铺行为数据）
    private static final String OUTPUT_TOPIC = ConfigUtils.getString("kafka.dwd.shop.behavior.topic");
    // 脏数据输出标签
    private static final OutputTag<String> DIRTY_TAG = new OutputTag<String>("dirty_data") {};

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取Kafka原始行为数据
        DataStreamSource<String> kafkaSource = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        ConfigUtils.getString("kafka.bootstrap.servers"),
                        INPUT_TOPIC,
                        "dwd_shop_customer_consumer",
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> JSONObject.parseObject(event).getLong("ts")),
                "shop_behavior_source"
        );
//        kafkaSource.print();
        // 数据清洗与转换
        SingleOutputStreamOperator<JSONObject> dwdStream = kafkaSource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject json = JSONObject.parseObject(value);
                    final JSONObject common = json.getJSONObject("common");
                    JSONObject result = new JSONObject();

                    // 提取公共字段
                    result.put("mid", common.getString("mid")); // 设备标识
                    result.put("uid", common.getString("uid")); // 用户ID
                    result.put("ar", common.getString("ar")); // 地区
                    result.put("ba", common.getString("ba")); // 手机品牌
                    result.put("md", common.getString("md")); // 手机型号
                    result.put("os", common.getString("os")); // 操作系统
                    result.put("ch", common.getString("ch")); // 渠道
                    result.put("vc", common.getString("vc")); // 版本
                    result.put("is_new", common.getString("is_new")); // 是否新用户
                    result.put("sid", common.getString("sid")); // 会话ID

                    // 提取页面信息
                    JSONObject page = json.getJSONObject("page");
                    if (page != null) {
                        result.put("page_id", page.getString("page_id")); // 页面ID
                        result.put("last_page_id", page.getString("last_page_id")); // 上一页ID
                        result.put("during_time", page.getLong("during_time")); // 页面停留时间
                        result.put("item", page.getString("item")); // 商品ID
                        result.put("item_type", page.getString("item_type")); // 商品类型
                    }

                    // 提取行为时间和类型
                    result.put("ts", json.getLong("ts")); // 行为时间
                    result.put("behavior_type", getBehaviorType(json)); // 行为类型

                    // 提取商品展示信息
                    JSONArray displays = json.getJSONArray("displays");
                    if (displays != null && !displays.isEmpty()) {
                        result.put("display_count", displays.size()); // 展示商品数量
                        // 记录用户最终操作的商品在展示列表中的位置
                        result.put("target_pos", getTargetPosition(displays, result.getString("item")));
                    }

                    // 过滤无效数据（缺少核心字段）
                    if (result.getString("mid") != null && result.getString("page_id") != null) {
                        out.collect(result);
                    } else {
                        ctx.output(DIRTY_TAG, value);
                    }
                } catch (Exception e) {
                    ctx.output(DIRTY_TAG, value); // 解析失败的脏数据
                }
            }

            // 获取目标商品在展示列表中的位置
            private int getTargetPosition(JSONArray displays, String targetItem) {
                if (displays == null || targetItem == null) return -1;

                for (int i = 0; i < displays.size(); i++) {
                    JSONObject display = displays.getJSONObject(i);
                    if (targetItem.equals(display.getString("item"))) {
                        return display.getIntValue("pos_seq");
                    }
                }
                return -1;
            }

            // 定义行为类型：浏览商品详情(good_detail)、添加购物车(cart_add)、浏览首页(home)、浏览我的(mine)等
            private String getBehaviorType(JSONObject json) {
                // 优先从actions中判断行为类型
                JSONArray actions = json.getJSONArray("actions");
                if (actions != null && !actions.isEmpty()) {
                    return actions.getJSONObject(0).getString("action_id");
                }

                // 从页面信息判断行为类型
                JSONObject page = json.getJSONObject("page");
                if (page != null) {
                    return page.getString("page_id");
                }

                return "unknown";
            }
        }).name("dwd_shop_behavior_clean");

        dwdStream.print();

        // 输出到Kafka
        dwdStream.map(JSONObject::toString)
                .sinkTo(KafkaUtils.buildKafkaSink(
                        ConfigUtils.getString("kafka.bootstrap.servers"),
                        OUTPUT_TOPIC
                )).name("dwd_shop_behavior_sink");

        // 脏数据处理
        dwdStream.getSideOutput(DIRTY_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(
                        ConfigUtils.getString("kafka.bootstrap.servers"),
                        ConfigUtils.getString("kafka.dirty.topic")
                )).name("dirty_data_sink");

        env.execute("DWD_Shop_Customer_Behavior_Process");
    }
}