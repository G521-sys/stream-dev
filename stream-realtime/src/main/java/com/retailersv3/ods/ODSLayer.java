package com.retailersv3.ods;

import com.retailersv3.founch.SearchLogSource;
import com.stream.common.utils.ConfigUtils;
import com.stream.common.utils.EnvironmentSettingUtils;
import com.stream.common.utils.KafkaUtils;
import lombok.SneakyThrows;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.util.Date;

// ODS层（操作数据存储层）处理类
public class ODSLayer {
    // 从配置文件获取Kafka的bootstrap.servers配置
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    // main方法，程序入口，使用SneakyThrows简化异常抛出
    @SneakyThrows
    public static void main(String[] args){
        // 设置Hadoop用户名（用于HDFS权限控制）
        System.setProperty("HADOOP_USER_NAME","root");
        // 获取Flink的流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 从自定义数据源SearchLogSource读取数据，得到原始数据流（String类型）
        DataStream<String> rawStream = env.addSource(new SearchLogSource());
        // 打印原始数据，前缀为"ODS Layer - Raw Data"
        rawStream.print("ODS Layer - Raw Data");
        rawStream.sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers,"ods_log"))
                .uid("source_ods_log")
                .name("source_ods_log");

        env.execute();
    }
}