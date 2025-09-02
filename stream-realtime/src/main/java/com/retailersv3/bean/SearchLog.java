package com.retailersv3.bean;

import java.io.Serializable;

// 搜索日志实体类，实现Serializable接口支持序列化
public class SearchLog implements Serializable {
    // 声明字段（查询ID、查询词、文档ID、商品标题、时间戳、数据类型）
    public int queryId;
    public String query;
    public int docId;
    public String title; // 商品标题
    public long timestamp;
    public String type; // "train" 或 "test"
    // 无参构造方法（反序列化需要）
    public SearchLog() {}
    // 有参构造方法，用于初始化所有字段
    public SearchLog(int queryId, String query, int docId, String title, long timestamp, String type) {
        this.queryId = queryId;
        this.query = query;
        this.docId = docId;
        this.title = title;
        this.timestamp = timestamp;
        this.type = type;
    }
    // 从CSV字符串解析为SearchLog对象的静态方法
    public static SearchLog fromCSV(String csv) {
        try {
            // 按逗号分割CSV字符串
            String[] parts = csv.split(",");
            SearchLog log = new SearchLog();
            // 解析各字段并赋值
            log.queryId = Integer.parseInt(parts[0]);
            log.query = parts[1];
            log.docId = Integer.parseInt(parts[2]);
            log.title = parts[3];
            log.timestamp = Long.parseLong(parts[4]);
            log.type = parts[5];
            return log;
        } catch (Exception e) {
            // 解析失败返回null
            return null;
        }
    }
    // 将对象转换为CSV字符串的方法
    public String toCSV() {
        return String.format("%d,%s,%d,%s,%d,%s", 
            queryId, query, docId, title, timestamp, type);
    }
    // 验证对象是否有效的方法（基本字段校验）
    public boolean isValid() {
        return queryId > 0 && docId > 0 && query != null && !query.isEmpty();
    }
    // 重写toString方法，返回CSV格式字符串
    @Override
    public String toString() {
        return toCSV();
    }
}