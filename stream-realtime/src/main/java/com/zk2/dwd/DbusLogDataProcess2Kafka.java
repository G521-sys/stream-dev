package com.zk2.dwd;

import com.alibaba.fastjson.JSONObject;
import com.retailersv1.func.ProcessSplitStreamFunc;
import com.stream.common.utils.*;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

public class DbusLogDataProcess2Kafka {

    private static final String kafka_topic_base_log_data = ConfigUtils.getString("REALTIME.KAFKA.LOG.TOPIC");
    private static final String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
    private static final String kafka_err_log = ConfigUtils.getString("kafka.err.log");
    private static final String kafka_start_log = ConfigUtils.getString("kafka.start.log");
    private static final String kafka_display_log = ConfigUtils.getString("kafka.display.log");
    private static final String kafka_action_log = ConfigUtils.getString("kafka.action.log");
    private static final String kafka_dirty_topic = ConfigUtils.getString("kafka.dirty.topic");
    private static final String kafka_page_topic = ConfigUtils.getString("kafka.page.topic");
    private static final OutputTag<String> errTag = new OutputTag<String>("errTag") {};
    private static final OutputTag<String> startTag = new OutputTag<String>("startTag") {};
    private static final OutputTag<String> displayTag = new OutputTag<String>("displayTag") {};
    private static final OutputTag<String> actionTag = new OutputTag<String>("actionTag") {};
    private static final OutputTag<String> dirtyTag = new OutputTag<String>("dirtyTag") {};
    private static final HashMap<String, DataStream<String>> collectDsMap = new HashMap<>();

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME","root");

        CommonUtils.printCheckPropEnv(
                false,
                kafka_topic_base_log_data,
                kafka_botstrap_servers,
                kafka_page_topic,
                kafka_err_log,
                kafka_start_log,
                kafka_display_log,
                kafka_action_log,
                kafka_dirty_topic
        );

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettingUtils.defaultParameter(env);

        env.setStateBackend(new MemoryStateBackend());

        DataStreamSource<String> kafkaSourceDs = env.fromSource(
                KafkaUtils.buildKafkaSource(
                        kafka_botstrap_servers,
                        kafka_topic_base_log_data,
                        "retailers_realtime_log_consumer",
                        OffsetsInitializer.earliest()
                ),
                WatermarkStrategy.noWatermarks(),
                "read_kafka_realtime_log"
        );
        kafkaSourceDs.print();
        SingleOutputStreamOperator<JSONObject> processDs = kafkaSourceDs.process(new ProcessFunction<String, JSONObject>() {
                    @Override
                    public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                        try {
                            collector.collect(JSONObject.parseObject(s));
                        } catch (Exception e) {
                            context.output(dirtyTag, s);
                            System.err.println("Convert JsonData Error !");
                        }
                    }
                }).uid("convert_json_process")
                .name("convert_json_process");

        SideOutputDataStream<String> dirtyDs = processDs.getSideOutput(dirtyTag);
        dirtyDs.print("dirtyDs ->");
        dirtyDs.sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_dirty_topic))
                .uid("sink_dirty_data_to_kafka")
                .name("sink_dirty_data_to_kafka");

        KeyedStream<JSONObject, String> keyedStream = processDs.keyBy(obj -> obj.getJSONObject("common").getString("mid"));
        SingleOutputStreamOperator<JSONObject> mapDs = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
                    private ValueState<String> lastVisitDateState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<String> valueStateDescriptor = new ValueStateDescriptor<>("lastVisitDateState", String.class);
                        valueStateDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10))
                                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                                .build());
                        lastVisitDateState = getRuntimeContext().getState(valueStateDescriptor);
                    }

                    @Override
                    public JSONObject map(JSONObject jsonObject) throws Exception {
                        String isNew = jsonObject.getJSONObject("common").getString("is_new");
                        String lastVisitDate = lastVisitDateState.value();
                        Long ts = jsonObject.getLong("ts");
                        String curVisitDate = DateTimeUtils.tsToDate(ts);
                        if ("1".equals(isNew)) {
                            if (StringsUtils.isEmpty(lastVisitDate)) {
                                lastVisitDateState.update(curVisitDate);
                            } else {
                                if (!lastVisitDate.equals(curVisitDate)) {
                                    isNew = "0";
                                    jsonObject.getJSONObject("common").put("is_new", isNew);
                                }
                            }
                        } else {
                            if (StringsUtils.isEmpty(lastVisitDate)) {
                                String yesDay = DateTimeUtils.tsToDate(ts - 24 * 60 * 60 * 1000);
                                lastVisitDateState.update(yesDay);
                            }
                        }
                        return jsonObject;
                    }

                    @Override
                    public void close() throws Exception {
                        super.close();
                    }
                }).uid("fix_isNew_map")
                .name("fix_isNew_map");

        SingleOutputStreamOperator<String> processTagDs = mapDs.process(new ProcessSplitStreamFunc(errTag,startTag,displayTag,actionTag))
                .uid("flag_stream_process")
                .name("flag_stream_process");

        SideOutputDataStream<String> sideOutputErrDS = processTagDs.getSideOutput(errTag);
        SideOutputDataStream<String> sideOutputStartDS = processTagDs.getSideOutput(startTag);
        SideOutputDataStream<String> sideOutputDisplayTagDS = processTagDs.getSideOutput(displayTag);
        SideOutputDataStream<String> sideOutputActionTagTagDS = processTagDs.getSideOutput(actionTag);

        collectDsMap.put("errTag",sideOutputErrDS);
        collectDsMap.put("startTag",sideOutputStartDS);
        collectDsMap.put("displayTag",sideOutputDisplayTagDS);
        collectDsMap.put("actionTag",sideOutputActionTagTagDS);
        collectDsMap.put("page",processTagDs);

        SplitDs2kafkaTopicMsg(collectDsMap);

        env.disableOperatorChaining();
        env.execute("Job-DbusLogDataProcess2Kafka");
    }
    public static void SplitDs2kafkaTopicMsg(Map<String,DataStream<String>> dataStreamHashMap){
        dataStreamHashMap.get("errTag").sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_err_log))
                .uid("sk_errMsg2Kafka")
                .name("sk_errMsg2Kafka");

        dataStreamHashMap.get("startTag").sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_start_log))
                .uid("sk_startMsg2Kafka")
                .name("sk_startMsg2Kafka");

        dataStreamHashMap.get("displayTag").sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_display_log))
                .uid("sk_displayMsg2Kafka")
                .name("sk_displayMsg2Kafka");

        dataStreamHashMap.get("actionTag").sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_action_log))
                .uid("sk_actionMsg2Kafka")
                .name("sk_actionMsg2Kafka");

        dataStreamHashMap.get("page").sinkTo(KafkaUtils.buildKafkaSink(kafka_botstrap_servers, kafka_page_topic))
                .uid("sk_pageMsg2Kafka")
                .name("sk_pageMsg2Kafka");

//        dataStreamHashMap.get("errTag").print("errTag ->");
//        dataStreamHashMap.get("startTag").print("startTag ->");
//        dataStreamHashMap.get("displayTag").print("displayTag ->");
//        dataStreamHashMap.get("actionTag").print("actionTag ->");
//        dataStreamHashMap.get("page").print("page ->");
    }
}


















