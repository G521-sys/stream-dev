package com.zk2.dwd;

import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class DwdVisitBehavior {
    // 定义Kafka主题
    private static final String KAFKA_DISPLAY_TOPIC = ConfigUtils.getString("kafka.display.log");
    private static final String KAFKA_DWD_VISIT_TOPIC = ConfigUtils.getString("kafka.dwd.visit.topic");
    private static final String KAFKA_BOOTSTRAP_SERVERS = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final OutputTag<String> DIRTY_TAG = new OutputTag<String>("dirtyData") {};

    public static void main(String[] args) throws Exception {
        // 1. 环境配置
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2. 读取display主题数据
        DataStreamSource<String> displaySource = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        KAFKA_BOOTSTRAP_SERVERS,
                        KAFKA_DISPLAY_TOPIC,
                        "dwd_visit_consumer",
                        org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withIdleness(Duration.ofSeconds(10)),
                "display_log_source"
        );
//        displaySource.print();
        // 3. 数据清洗与转换
        SingleOutputStreamOperator<JSONObject> dwdVisitStream = displaySource.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject json = JSONObject.parseObject(value);
                    JSONObject result = new JSONObject();

                    // 提取用户标识字段
                    result.put("uid", json.getJSONObject("common").getString("uid"));
                    result.put("mid", json.getJSONObject("common").getString("mid"));
                    result.put("sid", json.getJSONObject("common").getString("sid"));

                    // 提取行为字段
                    result.put("page_id", json.getJSONObject("page").getString("page_id"));
                    result.put("item", json.getJSONObject("page").getString("item"));
                    result.put("item_type", json.getJSONObject("page").getString("item_type"));

                    // 提取时间字段
                    result.put("ts", json.getLong("ts"));
                    result.put("visit_date", com.stream.common.utils.DateTimeUtils.tsToDate(json.getLong("ts"))); // 自定义日期转换工具

                    // 输出清洗后的数据
                    out.collect(result);
                } catch (Exception e) {
                    // 脏数据输出
                    ctx.output(DIRTY_TAG, value);
                }
            }
        }).name("clean_visit_data").uid("clean_visit_data");
        dwdVisitStream.print();
        // 4. 输出到DWD主题
        dwdVisitStream.map(JSONObject::toString)
                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_BOOTSTRAP_SERVERS, KAFKA_DWD_VISIT_TOPIC))
                .name("dwd_visit_sink").uid("dwd_visit_sink");

        // 5. 脏数据处理
        dwdVisitStream.getSideOutput(DIRTY_TAG)
                .sinkTo(KafkaUtils.buildKafkaSink(KAFKA_BOOTSTRAP_SERVERS, ConfigUtils.getString("kafka.dirty.topic")))
                .name("dirty_data_sink").uid("dirty_data_sink");

        env.execute("DWD Visit Behavior Processing");
    }
}