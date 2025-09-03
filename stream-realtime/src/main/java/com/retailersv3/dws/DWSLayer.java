package com.retailersv3.dws;

import com.alibaba.fastjson.JSONObject;
import com.retailersv3.bean.SearchLog;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

// DWS层（数据服务层）处理类 - 工单编号: 大数据-用户画像-19-阿里电商搜索
public class DWSLayer {

    // 从配置文件获取Kafka的bootstrap.servers配置
    private static final String kafka_bootstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");

    // QueryCount类，用于存储查询统计结果
    public static class QueryCount {
        public String query;
        public long count;
        public long windowStart;
        public long windowEnd;
        public long timestamp;

        public QueryCount() {}

        public QueryCount(String query, long count, long windowStart, long windowEnd, long timestamp) {
            this.query = query;
            this.count = count;
            this.windowStart = windowStart;
            this.windowEnd = windowEnd;
            this.timestamp = timestamp;
        }

        @Override
        public String toString() {
            return String.format("%s,%d,%d,%d,%d", query, count, windowStart, windowEnd, timestamp);
        }
    }

    @SneakyThrows
    public static void main(String[] args) {
        // 设置Hadoop用户名
        System.setProperty("HADOOP_USER_NAME", "root");

        // 获取Flink的流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 从DWD层Kafka主题读取数据
        KafkaSource<String> kafkaSource = KafkaUtils.buildKafkaSource(
                kafka_bootstrap_servers,
                "dwd_log",
                "dws_query_count_consumer_group",
                OffsetsInitializer.earliest()
        );

        DataStreamSource<String> dwdStreamSource = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "dwd_kafka_source"
        );

//        dwdStreamSource.print();
        // 转换为SearchLog对象
        final SingleOutputStreamOperator<SearchLog> searchLogStream = dwdStreamSource.map(s -> JSONObject.parseObject(s)).map(new MapFunction<JSONObject, SearchLog>() {
            @Override
            public SearchLog map(JSONObject s) throws Exception {
                final SearchLog searchLog = new SearchLog();
                searchLog.queryId = s.getInteger("shop_id");
                searchLog.query = s.getString("g_name");
                searchLog.docId = s.getInteger("g_id");
                searchLog.title = s.getString("shop_name");
                searchLog.timestamp = s.getLong("ts");
                searchLog.type = s.getString("labels");
                return searchLog;
            }
        }).filter(s -> s != null && s.isValid());
//        searchLogStream.print();
        // 按查询词进行窗口聚合统计
        SingleOutputStreamOperator<QueryCount> queryCountStream = searchLogStream
                .keyBy(s -> s.query)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                .reduce(new ReduceFunction<SearchLog>() {
                    @Override
                    public SearchLog reduce(SearchLog searchLog, SearchLog t1) throws Exception {
//                         简单的计数聚合，这里可以优化为仅保留一个对象并增加计数字段（如果SearchLog中有）
                        return searchLog;
                    }
                }, new ProcessWindowFunction<SearchLog, QueryCount, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<SearchLog, QueryCount, String, TimeWindow>.Context context, Iterable<SearchLog> elements, Collector<QueryCount> out) throws Exception {
                        long count = 0;
                        for (SearchLog ignored : elements) {
                            count++;
                        }
                        TimeWindow window = context.window();
                        QueryCount queryCount = new QueryCount(s, count, window.getStart(), window.getEnd(), System.currentTimeMillis());
                        out.collect(queryCount);
                    }
                });
//        queryCountStream.print();


        // 写入DWS层Kafka主题
        queryCountStream.map(QueryCount::toString)
                .sinkTo(KafkaUtils.buildKafkaSink(kafka_bootstrap_servers, "dws_query_count"))
                .uid("dws_query_count_sink")
                .name("dws_query_count_sink");

        env.execute("DWS Layer Processing - 工单编号: 大数据-用户画像-19-阿里电商搜索");
    }
}