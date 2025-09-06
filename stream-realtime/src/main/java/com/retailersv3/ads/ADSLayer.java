package com.retailersv3.ads;

import com.retailersv3.dws.DWSLayer;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;

import java.util.*;

public class ADSLayer {
    // 从配置文件获取Kafka的bootstrap.servers配置
    private static final String kafka_bootstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");

    public static class HotQuery {
        public String query;
        public long count;
        public long timestamp;
        public int rank;

        public HotQuery() {}

        public HotQuery(String query, long count, long timestamp, int rank) {
            this.query = query;
            this.count = count;
            this.timestamp = timestamp;
            this.rank = rank;
        }

        @Override
        public String toString() {
            return String.format("%s,%d,%d,%d", query, count, timestamp, rank);
        }
    }

    @SneakyThrows
    public static void main(String[] args) {
        // 获取Flink的流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从DWD层Kafka主题读取数据
        KafkaSource<String> kafkaSource = KafkaUtils.buildKafkaSource(
                kafka_bootstrap_servers,
                "dws_query_count",
                "dws_query_count_sink",
                OffsetsInitializer.earliest()
        );
        DataStreamSource<String> dwdStreamSource = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "dwd_kafka_source"
        );

        final SingleOutputStreamOperator<DWSLayer.QueryCount> streamOperator = dwdStreamSource.flatMap(new FlatMapFunction<String, DWSLayer.QueryCount>() {
            @Override
            public void flatMap(String s, Collector<DWSLayer.QueryCount> collector) throws Exception {
                String[] str = s.split(",");
                DWSLayer.QueryCount queryCount = new DWSLayer.QueryCount();
                queryCount.query = str[0];
                queryCount.count = Long.parseLong(str[1]);
                queryCount.windowStart = Long.parseLong(str[2]);
                queryCount.windowEnd = Long.parseLong(str[3]);
                queryCount.timestamp = Long.parseLong(str[4]);
                collector.collect(queryCount);
            }
        });
//        streamOperator.print();
        streamOperator.keyBy(s->"all")
                        .map(new RichMapFunction<DWSLayer.QueryCount, HotQuery>() {
                            private transient Map<String, Long> queryCountMap;
                            private transient List<Map.Entry<String, Long>> topQueries;

                            @Override
                            public void open(Configuration parameters) throws Exception {
                                queryCountMap = new HashMap<>();
                                topQueries = new ArrayList<>();
                            }

                            @Override
                            public HotQuery map(DWSLayer.QueryCount value) throws Exception {
                                // 更新计数
                                queryCountMap.put(value.query,
                                        queryCountMap.getOrDefault(value.query, 0L) + value.count);

                                // 排序获取TopN
                                topQueries = new ArrayList<>(queryCountMap.entrySet());
                                topQueries.sort((a, b) -> Long.compare(b.getValue(), a.getValue()));

                                // 只保留Top10
                                if (topQueries.size() > 10) {
                                    topQueries = topQueries.subList(0, 10);
                                }

                                // 返回热门查询（这里返回第一个作为示例）
                                if (!topQueries.isEmpty()) {
                                    Map.Entry<String, Long> top = topQueries.get(0);
                                    return new HotQuery(top.getKey(), top.getValue(),
                                            System.currentTimeMillis(), 1);
                                }

                                return null;
                            }
                        })
                .filter(query -> query != null).print();

        env.execute();
    }
}