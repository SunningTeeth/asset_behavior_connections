package org.daijb.huat.streaming;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.daijb.huat.AssetBehaviorConstants;
import org.daijb.huat.config.ModelParamsConfigurer;
import org.daijb.huat.entity.FlowSrcMatchAssetEntity;
import org.daijb.huat.services.*;
import org.daijb.huat.utils.DbConnectUtil;
import org.daijb.huat.utils.StringUtil;
import org.daijb.huat.config.JavaKafkaConfigurer;
import org.daijb.huat.entity.AssetSourceEntity;
import org.daijb.huat.entity.FlowEntity;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * @author daijb
 * @date 2021/2/8 14:13
 */
public class KafkaMessageStreaming implements AssetBehaviorConstants {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageStreaming.class);

    private volatile Map<String, Object> modelingParams;

    private volatile ServiceState state = ServiceState.Starting;

    private volatile boolean isFirstRunning = true;

    public static void main(String[] args) {
        KafkaMessageStreaming kafkaMessageStreaming = new KafkaMessageStreaming();
        kafkaMessageStreaming.modelingParams = ModelParamsConfigurer.getModelingParams();
        // 启动定时任务
        kafkaMessageStreaming.startTimerTask(args);
    }

    public void run(String[] args) {
        StreamExecutionEnvironment streamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        // 重试4次，每次间隔20s
        streamExecutionEnvironment.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, Time.of(20, TimeUnit.SECONDS)));
        streamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        EnvironmentSettings fsSettings = EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        //创建 TableEnvironment
        StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(streamExecutionEnvironment, fsSettings);

        //每隔10s进行启动一个检查点【设置checkpoint的周期】
        //streamExecutionEnvironment.enableCheckpointing(10000);
        //设置模式为：exactly_one，仅一次语义
        //streamExecutionEnvironment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //确保检查点之间有1s的时间间隔【checkpoint最小间隔】
        //streamExecutionEnvironment.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        //检查点必须在10s之内完成，或者被丢弃【checkpoint超时时间】
        //streamExecutionEnvironment.getCheckpointConfig().setCheckpointTimeout(10000);
        //同一时间只允许进行一次检查点
        //streamExecutionEnvironment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //表示一旦Flink程序被cancel后，会保留checkpoint数据，以便根据实际需要恢复到指定的checkpoint
        //streamExecutionEnvironment.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置statebackend,将检查点保存在hdfs上面，默认保存在内存中。这里先保存到本地
        //streamExecutionEnvironment.setStateBackend(new FsStateBackend("file:///Users/temp/cp/"));

        //加载kafka配置信息
        Properties kafkaProperties = JavaKafkaConfigurer.getKafkaProperties(args);
        logger.info("load kafka properties : " + kafkaProperties.toString());
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getProperty("bootstrap.servers"));
        //可g根据实际拉取数据等设置此值，默认30s
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000);
        //每次poll的最大数量
        //注意该值不要改得太大，如果poll太多数据，而不能在下次poll之前消费完，则会触发一次负载均衡，产生卡顿
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 30);
        //当前消费实例所属的消费组
        //属于同一个组的消费实例，会负载消费消息
        props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProperties.getProperty("group.id"));

        // 添加kafka source
        DataStream<FlowEntity> processStream = streamExecutionEnvironment.addSource(new FlinkKafkaConsumer010<>(kafkaProperties.getProperty("topic"), new SimpleStringSchema(), props))
                .process(new ParserKafkaProcessFunction());

        // 更新状态
        updateModelTaskStatus(ModelStatus.RUNNING);

        // 添加mysql.asset source
        DataStream<AssetSourceEntity> assetSourceProcessStream = streamExecutionEnvironment.addSource(new AssetRichSourceConfigurer()).process(new ParserAssetProcessFunction());

        // 注册UDF
        //日期转换函数: 将Flink Window Start/End Timestamp转换为指定时区时间(默认转换为北京时间)
        //streamTableEnvironment.registerFunction("UdfTimestampConverter", new UdfTimestampConverter());

        /**
         * kafka数据过滤含有资产id
         */
        DataStream<FlowEntity> kafkaFilterSourceStream = processStream.filter(new FilterFunction<FlowEntity>() {
            @Override
            public boolean filter(FlowEntity flowEntity) throws Exception {
                // 匹配含有资产的
                if (flowEntity == null || StringUtil.isEmpty(flowEntity.getDstId()) || StringUtil.isEmpty(flowEntity.getSrcId())) {
                    return false;
                }
                return true;
            }
        });

        // 注册kafka关联表
        streamTableEnvironment.createTemporaryView("kafka_source", kafkaFilterSourceStream, "srcId,srcIp,dstId,dstIp,areaId,flowId,rTime,rowtime.rowtime");

        // 注册asset关联表
        streamTableEnvironment.createTemporaryView("asset_source", assetSourceProcessStream, "entityId,entityName,assetIp,areaId");

        //Table assetSourceTable = streamTableEnvironment.sqlQuery("select entityId,entityName,assetIp,areaId from asset_source");
        //DataStream<AssetSourceEntity> assetSourceEntityDataStream = streamTableEnvironment.toAppendStream(assetSourceTable, AssetSourceEntity.class);

        // 运行sql
        String querySql = "select srcId,srcIp,dstId,dstIp,areaId,flowId,rTime " +
                " from kafka_source";

        String temporarySrcIdSql = "select entityId,assetIp,srcId,srcIp " +
                " from kafka_source ks,asset_source a " +
                " where ks.srcId = a.entityId ";

        String temporaryDstIdSql = "select entityId,assetIp,dstId,dstIp " +
                " from kafka_source ks,asset_source a " +
                " where ks.dstId = a.entityId ";

        // 获取结果
        Table kafkaSrcIdTable = streamTableEnvironment.sqlQuery(temporarySrcIdSql);
        Table kafkaDstIdTable = streamTableEnvironment.sqlQuery(temporaryDstIdSql);
        //Table table = streamTableEnvironment.sqlQuery(querySql);

        DataStream<FlowSrcMatchAssetEntity> flowEntityDataSrcStream = streamTableEnvironment.toAppendStream(kafkaSrcIdTable, FlowSrcMatchAssetEntity.class);
        //DataStream<FlowEntity> flowEntityDataDstStream = streamTableEnvironment.toAppendStream(kafkaDstIdTable, FlowEntity.class);

        //DataStream<FlowEntity> flowEntityDataStream = streamTableEnvironment.toAppendStream(table, FlowEntity.class);

        //全局唯一
        final AssetConnectionExecutive assetConnectionExecutive = new AssetConnectionExecutive();

        /**
         * 查找具有连接关系的数据
         */
        /*DataStream<FlowEntity> filter = flowEntityDataStream.filter(new FilterFunction<FlowEntity>() {
            @Override
            public boolean filter(FlowEntity flowEntity) throws Exception {
                return assetConnectionExecutive.assetBehaviorFilter(flowEntity);
            }
        });*/


        /**
         * 转换数据格式
         */
        DataStream<JSONObject> kafkaFlowSrcEntityJson = flowEntityDataSrcStream.map(new MapFunction<FlowSrcMatchAssetEntity, JSONObject>() {
            @Override
            public JSONObject map(FlowSrcMatchAssetEntity flowSrcMatchAssetEntity) throws Exception {
                JSONObject jsonObject = flowSrcMatchAssetEntity.toJSONObject();
                return jsonObject;
            }
        });

        kafkaFlowSrcEntityJson.addSink(new MySqlSink());

        /*DataStream<JSONObject> kafkaFlowDstEntityJson = flowEntityDataDstStream.map(new MapFunction<FlowEntity, JSONObject>() {
            @Override
            public JSONObject map(FlowEntity flowEntity) throws Exception {
                String jsonStr = JSONObject.toJSONString(flowEntity, SerializerFeature.WriteDateUseDateFormat);
                return JSONObject.parseObject(jsonStr);
            }
        });

        kafkaFlowDstEntityJson.addSink(new MySqlSink());*/

        //kafkaFlowSrcEntityJson.addSink(new MySqlSink());

        /**
         * sink 到hdfs
         */
        // Storage into hdfs
       /* BucketingSink<String> sink = new BucketingSink<String>("/data/twms/traffichuixing/test_topic");
        sink.setBucketer(new DateTimeBucketer<String>("yyyy-MM-dd", ZoneId.of("Asia/Shanghai")));
        sink.setBatchSize(1024 * 1024 * 1024L);
        // one hour producer a file into hdfs
        sink.setBatchRolloverInterval(1000 * 60 * 60L);
        sink.setPendingPrefix("");
        sink.setPendingSuffix("");
        sink.setInProgressPrefix(".");*/

        //.addSink(new MySqlTwoPhaseCommitSink()).name("MySqlTwoPhaseCommitSink");

        try {
            streamExecutionEnvironment.execute("kafka message streaming start ....");
        } catch (Exception e) {
            logger.error("flink streaming execute failed", e);
            // 更新状态
            updateModelTaskStatus(ModelStatus.STOP);
        }
    }

    private static class ParserAssetProcessFunction extends ProcessFunction<Tuple4<String, String, String, Integer>, AssetSourceEntity> {

        @Override
        public void processElement(Tuple4<String, String, String, Integer> value, Context ctx, Collector<AssetSourceEntity> out) throws Exception {
            AssetSourceEntity assetSourceEntity = new AssetSourceEntity();
            assetSourceEntity.setEntityId(value.f0);
            assetSourceEntity.setEntityName(value.f1);
            assetSourceEntity.setAssetIp(value.f2);
            assetSourceEntity.setAreaId(value.f3);
            out.collect(assetSourceEntity);
        }
    }

    /**
     * 解析kafka数据
     */
    private static class ParserKafkaProcessFunction extends ProcessFunction<String, FlowEntity> {

        @Override
        public void processElement(String value, Context ctx, Collector<FlowEntity> out) throws Exception {
            FlowEntity flowEntity = JSON.parseObject(value, FlowEntity.class);
            //输出到主流
            out.collect(flowEntity);
        }
    }

    /**
     * 更新建模参数
     */
    @SuppressWarnings("all")
    public void startTimerTask(String[] args) {
        final KafkaMessageStreaming kafkaMessageStreaming = this;

        /*scheduledExecutor.scheduleAtFixedRate(() -> {
            kafkaMessageStreaming.modelingParams = ModelParamsConfigurer.buildModelingParams();
        }, 1, 5, TimeUnit.MINUTES);

        scheduledExecutor.scheduleAtFixedRate(() -> {
            checkState(args);
        }, 0, 1, TimeUnit.MINUTES);*/

        Timer timer = new Timer();
        Calendar currentTime = Calendar.getInstance();
        currentTime.setTime(new Date());
        int delay = 5 - currentTime.get(Calendar.MINUTE) % 5;
        currentTime.set(Calendar.MINUTE, currentTime.get(Calendar.MINUTE) + delay);
        currentTime.set(Calendar.SECOND, 0);
        currentTime.set(Calendar.MILLISECOND, 0);

        Date firstTime = currentTime.getTime();

        // 每五分钟执行
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    kafkaMessageStreaming.modelingParams = AssetBehaviorBuildModelUtil.buildModelingParams();
                } catch (Throwable throwable) {
                    logger.error("timer schedule at fixed rate failed ", throwable);
                }
            }
        }, firstTime, 1000 * 60 * 5);

        // 每分钟执行
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    kafkaMessageStreaming.checkState(args);
                } catch (Throwable throwable) {
                    logger.error("check state task timer schedule failed ", throwable);
                }
            }
        }, 1000 * 20, 1000 * 60);

    }

    /**
     * 检查运行状态
     */
    private void checkState(String[] args) {
        if (this.modelingParams == null || this.modelingParams.isEmpty()) {
            state = ServiceState.Stopped;
        } else {
            state = ServiceState.Ready;
        }
        if (state == ServiceState.Ready && isFirstRunning) {
            isFirstRunning = false;
            logger.info("Flink Streaming Java Programming is starting...");
            this.run(args);
        }
    }

    /**
     * 更新建模状态
     *
     * @param modelStatus 状态枚举
     */
    private void updateModelTaskStatus(ModelStatus modelStatus) {
        String updateSql = "UPDATE `modeling_params` SET `model_task_status`=?, `modify_time`=? WHERE (`id`='" + modelingParams.get(MODEL_ID) + "');";
        DbConnectUtil.execUpdateTask(updateSql, modelStatus.toString().toLowerCase(), LocalDateTime.now().toString());
        logger.info("[kafkaMessageStreaming] update model task status : " + modelStatus.name());
    }

}
