package org.daijb.huat.config;

import org.apache.flink.api.java.utils.ParameterTool;
import org.daijb.huat.utils.UUIDUtil;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.UUID;

/**
 * @author daijb
 * @date 2021/2/9 10:43
 * 加载自定义kafka配置信息
 */
public class JavaKafkaConfigurer {

    public static void main(String[] args) {
        //  {"00:00:00:00:00:00":["192.168.8.97"]}
        String string = "{\"00:00:00:00:00:00\":[\"192.168.8.97\"]}";
        int start = string.indexOf("[") + 1;
        int end = string.indexOf("]");

        System.out.println(string.substring(start, end));

        String string1 = UUIDUtil.genId();

        String s = "[{\"16\":[192.168.9.231]}]";
        Object parse = JSONValue.parse(s);
        System.out.println(parse);
    }

    private static final Logger logger = LoggerFactory.getLogger(JavaKafkaConfigurer.class);

    private static volatile Properties properties;

    public static Properties getKafkaProperties(String[] args) {
        if (null == properties) {
            synchronized (JavaKafkaConfigurer.class) {
                if (properties == null) {
                    if (args == null || args.length <= 0) {
                        //获取配置文件kafka.properties的内容
                        Properties kafkaProperties = new Properties();
                        try {
                            kafkaProperties.load(JavaKafkaConfigurer.class.getClassLoader().getResourceAsStream("kafka.properties"));
                        } catch (Throwable throwable) {
                            logger.error("load kafka configurer failed due to ", throwable);
                        }
                        properties = kafkaProperties;
                    } else {
                        ParameterTool parameters = ParameterTool.fromArgs(args);
                        properties = parameters.getProperties();
                    }

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
            System.setProperty("java.security.auth.login.config", getKafkaProperties(null).getProperty("java.security.auth.login.config"));
        }
    }
}
