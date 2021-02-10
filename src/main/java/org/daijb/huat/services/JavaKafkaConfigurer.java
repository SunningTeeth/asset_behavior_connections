package org.daijb.huat.services;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;

/**
 * @author daijb
 * @date 2021/2/9 10:43
 * 加载自定义kafka配置信息
 */
public class JavaKafkaConfigurer {

    private static final Logger logger = LoggerFactory.getLogger(JavaKafkaConfigurer.class);

    private static volatile Properties properties;

    public static void main(String[] args) {
        getKafkaProperties();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            System.out.println(entry.getKey() + "=" + entry.getValue());
        }
    }

    public static Properties getKafkaProperties() {
        if (null == properties) {
            synchronized (JavaKafkaConfigurer.class) {
                if (properties == null) {
                    //获取配置文件kafka.properties的内容
                    Properties kafkaProperties = new Properties();
                    try {
                        kafkaProperties.load(JavaKafkaConfigurer.class.getClassLoader().getResourceAsStream("kafka.properties"));
                    } catch (Throwable throwable) {
                        logger.error("load kafka configurer failed due to ", throwable);
                    }
                    properties = kafkaProperties;
                }
            }
        }
        return properties;
    }

    public static void configureSecurityAuthLoginConfig() {
        //如果用-D或者其它方式设置过，这里不再设置
        if (null == System.getProperty("java.security.auth.login.config")) {
            //请注意将XXX修改为自己的路径
            //这个路径必须是一个文件系统可读的路径，不能被打包到jar中
            System.setProperty("java.security.auth.login.config", getKafkaProperties().getProperty("java.security.auth.login.config"));
        }
    }
}