package com.retailersv3.ads;

import com.retailersv3.dws.DWSLayer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import java.util.*;

public class ADSLayer {
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

    public static void process(DataStream<DWSLayer.QueryCount> dwsStream) {
        // 热门查询分析
        DataStream<HotQuery> hotQueryStream = dwsStream
                .keyBy(qc -> "all")
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
                .filter(query -> query != null);

        // 写入ADS层（Kafka）
        hotQueryStream.map(HotQuery::toString)
                .addSink(new FlinkKafkaProducer<>(
                        "localhost:9092",
                        "ads_hot_query",
                        new SimpleStringSchema()
                ));

        // 打印监控
        hotQueryStream.print("ADS Layer - Hot Query");
    }
    public static void main(String[] args) {

    }
}