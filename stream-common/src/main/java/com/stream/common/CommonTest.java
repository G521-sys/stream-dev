package com.stream.common;


import com.stream.common.utils.ConfigUtils;

public class CommonTest {

    public static void main(String[] args) {
        String kafka_err_log = ConfigUtils.getString("kafka.err.log");
        System.err.println(kafka_err_log);
    }


}
