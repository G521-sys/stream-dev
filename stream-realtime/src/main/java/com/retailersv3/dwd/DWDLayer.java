package com.retailersv3.dwd;

import com.alibaba.fastjson.JSONObject;
import com.retailersv3.bean.SearchLog;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

public class DWDLayer {
    // 从配置文件获取Kafka的bootstrap.servers配置
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");

    @SneakyThrows
    public static void main(String[] args) {
        // 获取Flink的流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettingUtils.defaultParameter(env);
        DataStreamSource<String> ods_log = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafka_botstrap_servers,
                        "ods_log",
                        "read_ods_log",
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "read_kafka_ods_log"
        );
        ods_log.print();
        final SingleOutputStreamOperator<JSONObject> json_log = ods_log.map(new MapFunction<String, JSONObject>() {
            @Override
            public JSONObject map(String s) throws Exception {
                try {
                    final String[] str = s.split(",");
                    final JSONObject jsonObject = new JSONObject();
                    jsonObject.put("shop_id", str[0]);
                    jsonObject.put("shop_name", str[1]);
                    jsonObject.put("g_id", str[2]);
                    jsonObject.put("g_name", str[3]);
                    jsonObject.put("ts", str[4]);
                    jsonObject.put("labels", str[4]);
                    return jsonObject;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }
        });
        json_log.print();

        json_log.map(s->s.toString()).sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers,"dwd_log"))
                .uid("source_dwd_log")
                .name("source_dwd_log");

        env.execute();
    }
}