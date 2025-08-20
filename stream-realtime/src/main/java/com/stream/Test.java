package com.stream;

import com.stream.common.utils.ConfigUtils;
import lombok.SneakyThrows;

/**
 * @Package com.stream.Test
 * @Author zhou.han
 * @Date 2024/12/29 22:45
 * @description:
 */
public class Test {
    @SneakyThrows
    public static void main(String[] args) {
        String kafka_botstrap_servers = ConfigUtils.getString("kafka.bootstrap.servers");
        System.err.println(kafka_botstrap_servers);
    }

}
