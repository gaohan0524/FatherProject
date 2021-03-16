package com.gaohan.hadoop.hdfs.wc;

import java.io.IOException;
import java.util.Properties;

public class ParamsUtil {

    private static Properties properties = new Properties();

    static {
        try {
            properties.load(ParamsUtil.class.getClassLoader().getResourceAsStream("wc.properties"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Properties getProperties() {
        return properties;
    }

    public static void main(String[] args) {
        System.out.println(getProperties().getProperty(Constants.MAPPER_CLASS));
    }
}
