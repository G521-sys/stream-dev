package com.retailersv1;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;

/**
 * 将MySQL维度表数据导入HBase的工具类
 * 支持的表: dim_activity_info, dim_activity_rule, dim_activity_sku,
 *          dim_base_category1/2/3, dim_base_dic, dim_base_province,
 *          dim_base_region, dim_base_trademark, dim_coupon_info,
 *          dim_coupon_range, dim_financial_sku_cost, dim_sku_info,
 *          dim_spu_info, dim_user_info
 */
public class MysqlToHBaseImporter {
    // MySQL连接信息
    private static final String MYSQL_URL = "jdbc:mysql://cdh01:3306/realtime_v1?useSSL=false&serverTimezone=UTC";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "000000";

    // HBase配置
    private static Configuration conf;
    private static Connection hbaseConnection;

    // 表名映射: MySQL表名 -> HBase表名
    private static final Map<String, String> TABLE_MAPPING = new HashMap<String, String>() {{
        put("dim_activity_info", "dim:activity_info");
        put("dim_activity_rule", "dim:activity_rule");
        put("dim_activity_sku", "dim:activity_sku");
        put("dim_base_category1", "dim:base_category1");
        put("dim_base_category2", "dim:base_category2");
        put("dim_base_category3", "dim:base_category3");
        put("dim_base_dic", "dim:base_dic");
        put("dim_base_province", "dim:base_province");
        put("dim_base_region", "dim:base_region");
        put("dim_base_trademark", "dim:base_trademark");
        put("dim_coupon_info", "dim:coupon_info");
        put("dim_coupon_range", "dim:coupon_range");
        put("dim_financial_sku_cost", "dim:financial_sku_cost");
        put("dim_sku_info", "dim:sku_info");
        put("dim_spu_info", "dim:spu_info");
        put("dim_user_info", "dim:user_info");
    }};

    // 列族配置
    private static final String COLUMN_FAMILY = "info";

    public static void main(String[] args) {
        try {
            // 初始化HBase配置
            initHBaseConfig();

            // 加载MySQL驱动
            Class.forName("com.mysql.cj.jdbc.Driver");

            // 循环处理所有表
            for (String mysqlTable : TABLE_MAPPING.keySet()) {
                String hbaseTable = TABLE_MAPPING.get(mysqlTable);
                System.out.println("开始导入表: " + mysqlTable + " 到 " + hbaseTable);
                importTable(mysqlTable, hbaseTable);
                System.out.println("表 " + mysqlTable + " 导入完成");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭HBase连接
            try {
                if (hbaseConnection != null) {
                    hbaseConnection.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 初始化HBase配置
     */
    private static void initHBaseConfig() throws Exception {
        conf = HBaseConfiguration.create();
        // 根据实际环境配置ZooKeeper地址
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseConnection = ConnectionFactory.createConnection(conf);
    }

    /**
     * 导入单个表数据
     * @param mysqlTable MySQL表名
     * @param hbaseTable HBase表名
     */
    private static void importTable(String mysqlTable, String hbaseTable) throws Exception {
        // 获取HBase表对象
        Table table = hbaseConnection.getTable(TableName.valueOf(hbaseTable));

        // 获取MySQL连接
        try (java.sql.Connection mysqlConn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
             Statement stmt = mysqlConn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM " + mysqlTable)) {

            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            // 批量处理计数器
            int batchSize = 1000;
            int count = 0;

            while (rs.next()) {
                // 获取主键作为HBase的RowKey
                // 假设每个表的主键列名为"id"
                String rowKey = rs.getString("id");
                Put put = new Put(Bytes.toBytes(rowKey));

                // 遍历所有列，添加到Put对象
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnName(i);
                    String value = rs.getString(i);

                    // 跳过空值
                    if (value == null) {
                        continue;
                    }

                    // 添加列数据到Put对象
                    put.addColumn(
                            Bytes.toBytes(COLUMN_FAMILY),
                            Bytes.toBytes(columnName),
                            Bytes.toBytes(value)
                    );
                }

                // 添加到批处理
                table.put(put);
                count++;

                // 每batchSize条数据提交一次
                if (count % batchSize == 0) {
//                    table.flushCommits();
                    System.out.println("已导入 " + count + " 条记录到 " + hbaseTable);
                }
            }

            // 提交剩余数据
//            table.flushCommits();
            System.out.println("表 " + mysqlTable + " 共导入 " + count + " 条记录");

        } finally {
            if (table != null) {
                table.close();
            }
        }
    }
}
