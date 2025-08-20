package com.retailersv1;
import lombok.SneakyThrows;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import com.alibaba.fastjson.JSONObject;
import com.stream.common.utils.ConfigUtils;
import com.stream.utils.CdcSourceUtils;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
public class MysqlcdcHbase {
    private static final String CDH_HBASE_NAME_SPACE = ConfigUtils.getString("hbase.namespace");

    // HBase配置
    private static final String PRIMARY_KEY = ConfigUtils.getString("mysql.primary.key", "id");
    private static final String HBASE_TABLE = "hbase.namespace";
    private static final String HBASE_COLUMN_FAMILY = "info";
    private static final String ZOOKEEPER_QUORUM = "cdh01";
    private static final String ZOOKEEPER_CLIENT_PORT = "2181";
    private static final int BATCH_SIZE = ConfigUtils.getInt("hbase.batch.size", 100);
    @SneakyThrows
    public static void main(String[] args) {
        Connection mysqlConn = null;
        Connection hbaseConn = null;
        Table hbaseTable = null;
        // 1. 设置 Hadoop 用户名（解决 HBase/Kafka 访问权限问题，避免权限校验失败）
        System.setProperty("HADOOP_USER_NAME","root");
        // 2. 初始化 Flink 流处理环境（默认使用本地/集群环境，注释的代码用于加载默认参数如并行度、检查点等）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 业务库 CDC 数据源
        MySqlSource<String> mySQLDbMainCdcSource = CdcSourceUtils.getMySQLCdcSource(
                ConfigUtils.getString("mysql.database"),
                "",
                ConfigUtils.getString("mysql.user"),
                ConfigUtils.getString("mysql.pwd"),
                StartupOptions.initial()
        );
        DataStreamSource<String> cdcDbMainStream = env.fromSource(mySQLDbMainCdcSource, WatermarkStrategy.noWatermarks(), "mysql_cdc_main_source");
        //读取到的Mysql的数据转换成json的形式
        SingleOutputStreamOperator<JSONObject> cdcDbMainStreamMap = cdcDbMainStream.map(JSONObject::parseObject)
                .uid("db_data_convert_json")
                .name("db_data_convert_json")
                .setParallelism(1);
//        cdcDbMainStreamMap.print();
        cdcDbMainStreamMap.addSink(new HBaseDimSink())
                .uid("json_to_hbase")
                .name("json_to_hbase")
                .setParallelism(1);

        // 执行任务
        env.execute("MySQL CDC 维度表同步到 HBase");

    }
    public static class HBaseDimSink extends RichSinkFunction<JSONObject>{
        private transient Connection hbaseConn;
        private transient Table hbaseTable;
        private List<Put> batchPuts;
        private long lastFlushTime;
        @Override
        public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
            // 初始化HBase连接
            Configuration conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
            conf.set("hbase.zookeeper.property.clientPort", ZOOKEEPER_CLIENT_PORT);
            hbaseConn = ConnectionFactory.createConnection(conf);
            hbaseTable = hbaseConn.getTable(TableName.valueOf(HBASE_TABLE));

            // 初始化批量集合
            batchPuts = new ArrayList<>(BATCH_SIZE);
            lastFlushTime = System.currentTimeMillis();
        }

        @Override
        public void invoke(JSONObject json, Context context) throws Exception {
            // 处理CDC数据
            String op = json.getString("op");
            JSONObject data = json.getJSONObject("after");

            // 根据操作类型处理
            switch (op) {
                case "c":  // 新增
                case "u":  // 更新
                    if (data != null) {
                        Put put = createPutFromJson(data);
                        batchPuts.add(put);
                    }
                    break;
                case "d":  // 删除
                    JSONObject beforeData = json.getJSONObject("before");
                    if (beforeData != null) {
                        Delete delete = createDeleteFromJson(beforeData);
                        hbaseTable.delete(delete);
                    }
                    break;
                case "r":  // 初始化读取
                    if (data != null) {
                        Put put = createPutFromJson(data);
                        batchPuts.add(put);
                    }
                    break;
            }

            // 批量刷新条件：达到批次大小或超过5秒
            if (batchPuts.size() >= BATCH_SIZE ||
                    System.currentTimeMillis() - lastFlushTime > TimeUnit.SECONDS.toMillis(5)) {
                flush();
            }
        }

        /**
         * 将JSON数据转换为HBase的Put对象
         */
        private Put createPutFromJson(JSONObject data) {
            // 使用主键生成RowKey
            String primaryValue = data.getString(PRIMARY_KEY);
            // 对主键进行MD5哈希处理，避免热点问题
            String rowKey = MD5Hash.getMD5AsHex(primaryValue.getBytes(StandardCharsets.UTF_8))
                    + "_" + primaryValue;

            Put put = new Put(Bytes.toBytes(rowKey));

            // 遍历所有字段，添加到Put对象
            for (String key : data.keySet()) {
                String value = data.getString(key);
                if (value != null) {
                    put.addColumn(
                            Bytes.toBytes(HBASE_COLUMN_FAMILY),
                            Bytes.toBytes(key),
                            Bytes.toBytes(value)
                    );
                }
            }
            return put;
        }

        /**
         * 创建删除操作
         */
        private Delete createDeleteFromJson(JSONObject data) {
            String primaryValue = data.getString(PRIMARY_KEY);
            String rowKey = MD5Hash.getMD5AsHex(primaryValue.getBytes(StandardCharsets.UTF_8))
                    + "_" + primaryValue;

            return new Delete(Bytes.toBytes(rowKey));
        }

        /**
         * 批量写入HBase
         */
        private void flush() throws Exception {
            if (!batchPuts.isEmpty()) {
                hbaseTable.put(batchPuts);
                System.out.println("成功写入 " + batchPuts.size() + " 条数据到HBase表: " + HBASE_TABLE);
                batchPuts.clear();
                lastFlushTime = System.currentTimeMillis();
            }
        }

        @Override
        public void close() throws Exception {
            // 关闭前刷新剩余数据
            flush();

            // 关闭资源
            if (hbaseTable != null) {
                hbaseTable.close();
            }
            if (hbaseConn != null) {
                hbaseConn.close();
            }
        }
    }
}
