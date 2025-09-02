package com.retailersv3.dws;

import com.retailersv3.bean.SearchLog;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

public class DWSLayer {
    // 从配置文件获取Kafka的bootstrap.servers配置
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");

    public static class QueryCount {
        public String query;
        public long count;
        public long windowStart;
        public long windowEnd;

        public QueryCount() {}

        public QueryCount(String query, long count, long windowStart, long windowEnd) {
            this.query = query;
            this.count = count;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
        }

        @Override
        public String toString() {
            return String.format("%s,%d,%d,%d", query, count, windowStart, windowEnd);
        }
    }

    @SneakyThrows
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> dwd_log = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafka_botstrap_servers,
                        "dwd_log",
                        "read_ods_log",
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "read_kafka_ods_log"
        );

        DataStream<SearchLog> searchLogStream = dwd_log
            .map(SearchLog::fromCSV)
            .filter(SearchLog::isValid);

        final SingleOutputStreamOperator<QueryCount> queryCountStream = searchLogStream
                .keyBy(log -> log.query)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(30)))
                .reduce(new ReduceFunction<SearchLog>() {
                    @Override
                    public SearchLog reduce(SearchLog value1, SearchLog value2) throws Exception {
                        // 这里简单计数，实际中可以更复杂的聚合
                        return value1; // 仅示例
                    }
                })
                .map(log -> new QueryCount(log.query, 1, System.currentTimeMillis() - 30000, System.currentTimeMillis()))
                .keyBy(qc -> qc.query)
                .reduce(new ReduceFunction<QueryCount>() {
                    @Override
                    public QueryCount reduce(QueryCount value1, QueryCount value2) throws Exception {
                        return new QueryCount(value1.query, value1.count + value2.count,
                                Math.min(value1.windowStart, value2.windowStart),
                                Math.max(value1.windowEnd, value2.windowEnd));
                    }
                });


//        // 写入DWS层（Kafka）
//        queryCountStream.map(QueryCount::toString)
//            .addSink(new FlinkKafkaProducer<>(
//                kafka_botstrap_servers,
//                "dws_query_count",
//                new SimpleStringSchema()
//            ));
        // 打印监控

        queryCountStream.print("DWS Layer - Query Count");
        env.execute();
    }
}