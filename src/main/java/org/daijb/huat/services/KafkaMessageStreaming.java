package org.daijb.huat.services;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.daijb.huat.services.entity.FlowEntity;
import org.daijb.huat.services.utils.StringUtil;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * @author daijb
 * @date 2021/2/8 14:13
 */
public class KafkaMessageStreaming {

    public static void main(String[] args) throws Exception {
        KafkaMessageStreaming kafkaMessageStreaming = new KafkaMessageStreaming();
        kafkaMessageStreaming.run();
    }

    public void run() throws Exception {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        //创建 TableEnvironment
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment, fsSettings);

        //加载kafka.properties
        Properties kafkaProperties = JavaKafkaConfigurer.getKafkaProperties();

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getProperty("bootstrap.servers"));
        //可更加实际拉去数据和客户的版本等设置此值，默认30s
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        //每次poll的最大数量
        //注意该值不要改得太大，如果poll太多数据，而不能在下次poll之前消费完，则会触发一次负载均衡，产生卡顿
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 30);
        //当前消费实例所属的消费组
        //属于同一个组的消费实例，会负载消费消息
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getProperty("group.id"));

        FlinkKafkaConsumer010 kafkaConsumer = new FlinkKafkaConsumer010<>(kafkaProperties.getProperty("topic"), new SimpleStringSchema(), props);

        SingleOutputStreamOperator processStream = streamExecutionEnvironment.addSource(kafkaConsumer)
                .process(new ParserKafkaProcessFunction());

        /**
         * 数据过滤
         */
        DataStream<FlowEntity> filterSource = processStream.filter(new FilterFunction<FlowEntity>() {

            @Override
            public boolean filter(FlowEntity flowEntity) throws Exception {
                // 匹配含有资产的
                if (StringUtil.isEmpty(flowEntity.getDstId()) || StringUtil.isEmpty(flowEntity.getSrcId())) {
                    return false;
                }
                return true;
            }
        });

        /**
         * 转换数据格式
         */
        DataStream<JSONObject> kafkaJson = filterSource.map(new MapFunction<FlowEntity, JSONObject>() {
            @Override
            public JSONObject map(FlowEntity flowEntity) throws Exception {
                String jsonStr = JSONObject.toJSONString(flowEntity);
                return JSONObject.parseObject(jsonStr);
            }
        });

        // 创建临时试图表
        streamTableEnvironment.createTemporaryView("kafka_source", kafkaJson, "SrcID,SrcIP,DstID,DstIP,AreaID,FlowID,@timestamp");

        // 注册UDF
        streamTableEnvironment.registerFunction("UdfTimestampConverter", new UdfTimestampConverter());

        // 运行sql
        String queryExpr = "select SrcID as srcId,SrcIP as srcIp,DstIP as dstIp,DstIp as dstIp,AreaID as areaId,FlowID as flowId,@timestamp" +
                "from kafka_source;";

        // 获取结果
        Table table = streamTableEnvironment.sqlQuery(queryExpr);
        //table.distinct().

        /*streamExecutionEnvironment.addSource(kafkaConsumer)
                .print()
                .setParallelism(1);*/

        streamExecutionEnvironment.execute("kafka message streaming start ....");
    }

    /**
     * 解析kafka数据
     */
    private static class ParserKafkaProcessFunction extends ProcessFunction<String, FlowEntity> {

        @Override
        public void processElement(String value, Context ctx, Collector<FlowEntity> out) throws Exception {
            //输出到主流
            out.collect(JSON.parseObject(value, FlowEntity.class));
            // 输出到侧输出流
            //ctx.output(new OutputTag<>());
        }
    }

    /**
     * 自定义UDF
     */
    public static class UdfTimestampConverter extends ScalarFunction {

        /**
         * 默认转换为北京时间
         *
         * @param timestamp flink Timestamp 格式时间
         * @param format    目标格式,如"YYYY-MM-dd HH:mm:ss"
         * @return 目标时区的时间
         */
        public String eval(Timestamp timestamp, String format) {

            LocalDateTime noZoneDateTime = timestamp.toLocalDateTime();
            ZonedDateTime utcZoneDateTime = ZonedDateTime.of(noZoneDateTime, ZoneId.of("UTC"));

            ZonedDateTime targetZoneDateTime = utcZoneDateTime.withZoneSameInstant(ZoneId.of("+08:00"));

            return targetZoneDateTime.format(DateTimeFormatter.ofPattern(format));
        }

        /**
         * 转换为指定时区时间
         *
         * @param timestamp  flink Timestamp 格式时间
         * @param format     目标格式,如"YYYY-MM-dd HH:mm:ss"
         * @param zoneOffset 目标时区偏移量
         * @return 目标时区的时间
         */
        public String eval(Timestamp timestamp, String format, String zoneOffset) {

            LocalDateTime noZoneDateTime = timestamp.toLocalDateTime();
            ZonedDateTime utcZoneDateTime = ZonedDateTime.of(noZoneDateTime, ZoneId.of("UTC"));

            ZonedDateTime targetZoneDateTime = utcZoneDateTime.withZoneSameInstant(ZoneId.of(zoneOffset));

            return targetZoneDateTime.format(DateTimeFormatter.ofPattern(format));
        }
    }

}
