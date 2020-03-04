package com.imooc.bigdata.hadoop.hdfs.common;

import java.io.IOException;
import java.util.Objects;
import java.util.Properties;

public class PropertiesUtil {
    private PropertiesUtil() {
    }

    private static Properties properties = new Properties();

    static {
        try {
            properties.load(Objects.requireNonNull(PropertiesUtil.class.getClassLoader().getResourceAsStream("conf.properties")));
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public static Properties getProperties() {
        return properties;
    }
}
