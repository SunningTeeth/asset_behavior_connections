package org.daijb.huat.services;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.calcite.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.daijb.huat.services.entity.AssetBehaviorSink;
import org.daijb.huat.services.utils.ConversionUtil;
import org.daijb.huat.services.utils.DBConnectUtil;
import org.daijb.huat.services.utils.StringUtil;
import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.*;

import static org.daijb.huat.services.AssetBehaviorConstants.MODEL_ID;

/**
 * @author daijb
 * @date 2021/2/22 15:09
 */
public class MySqlSink extends RichSinkFunction<JSONObject> {

    private static final Logger logger = LoggerFactory.getLogger(MySqlSink.class);

    private PreparedStatement ps;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = DBConnectUtil.getConnection();
        String sql = "insert into `model_result_asset_behavior_relation` (`modeling_params_id`,`src_id`,`src_ip`,`dst_ip_segment`,`time`) values (?,?,?,?,?)";
        ps = connection.prepareStatement(sql);
        startThreadFunc();
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    @Override
    public void invoke(JSONObject json, Context context) throws Exception {
        String key = AssetBehaviorBuildModelUtil.calculateSegmentKey() + "";
        //addData(modelingParams, json, false, key);
        ps.setString(1, AssetBehaviorBuildModelUtil.getModelingParams().get("modelId").toString());
        ps.setString(2, json.getString("srcId"));
        ps.setString(3, json.getString("srcIp"));
        ps.setString(4, "daijb");
        ps.setString(5, LocalDateTime.now().toString());
        //执行insert语句
        //ps.addBatch();
        ps.execute();
    }


    private volatile long lastUpdateTime = 0;
    private volatile boolean isFirst = true;

    private volatile Map<String, Object> modelingParams;

    /**
     * 更新间隔
     */
    public final long INTERVAL = 60 * 60 * 1000;

    private ConcurrentMap<String, AssetBehaviorSink> allAssetBehaviorSink = new ConcurrentHashMap<>();

    public void startThreadFunc() {
        if (isFirst) {
            String buildModelRate = AssetBehaviorBuildModelUtil.getBuildModelRate().trim();
            String unit = buildModelRate.substring(buildModelRate.length() - 2);
            int rate = ConversionUtil.toInteger(buildModelRate.substring(0, buildModelRate.length() - 2));
            TimeUnit timeUnit = TimeUnit.HOURS;
            if (StringUtil.equals(unit, "hh")) {
                timeUnit = TimeUnit.DAYS;
            }
            System.out.println("开始定时任务.....");
            startTimerFunc(rate, timeUnit);
        }
    }

    public void addData(Map<String, Object> modelingParams, JSONObject json, boolean isSrc, String key) throws Exception {
        String entityId = null, assetIp = null;
        if (isSrc) {
            entityId = ConversionUtil.toString(json.get("srcId"));
            assetIp = ConversionUtil.toString(json.get("srcIp"));
        } else {
            entityId = ConversionUtil.toString(json.get("dstId"));
            assetIp = ConversionUtil.toString(json.get("dstIp"));
        }
        AssetBehaviorSink entity = allAssetBehaviorSink.get(entityId);
        String modelId = modelingParams.get(MODEL_ID).toString();
        if (entity == null) {
            JSONArray dstIpSegment = new JSONArray();
            JSONObject item = new JSONObject();
            item.put("name", key);
            item.put("value", assetIp);
            dstIpSegment.add(item);
            allAssetBehaviorSink.put(entityId, new AssetBehaviorSink(modelId, entityId, assetIp, dstIpSegment));
        } else {
            // 更新(多久更新)
            JSONArray dstIpSegment = entity.getDstIpSegment();
            for (int i = 0; i < dstIpSegment.size(); i++) {
                JSONObject obj = (JSONObject) dstIpSegment.get(i);
                if (StringUtil.equals(ConversionUtil.toString(obj.get("name")), key)) {
                    obj.put("value", obj.get("value") + "," + assetIp);
                    break;
                }
            }
            entity.setDstIpSegment(dstIpSegment);
            allAssetBehaviorSink.put(entityId, entity);
        }

    }

    private void startTimerFunc(int delay, TimeUnit timeUnit) {
        System.out.println("定时任务启动 delay : " + delay + ", timeUnit " + timeUnit);
        Executors.newScheduledThreadPool(1).scheduleWithFixedDelay(() -> {
            try {
                modelingParams = AssetBehaviorBuildModelUtil.buildModelingParams();
            } catch (Throwable throwable) {
                logger.error("timer schedule at fixed rate failed ", throwable);
            }

        }, 0, 5, TimeUnit.MINUTES);

        Executors.newScheduledThreadPool(3).scheduleWithFixedDelay(() -> {
            isFirst = false;
            try {
                startPrepareData();
            } catch (Throwable throwable) {
            }
        }, 0, delay, timeUnit);
    }

    private void startPrepareData() throws Exception {
        final Map<String, AssetBehaviorSink> assetBehaviorSinkMap = allAssetBehaviorSink;
        System.out.println("准备数据 , size : " + assetBehaviorSinkMap.size());
        if (allAssetBehaviorSink.size() > 4096 || lastUpdateTime < System.currentTimeMillis() - INTERVAL) {
            lastUpdateTime = System.currentTimeMillis();
            boolean hasData = false;
            for (AssetBehaviorSink entity : assetBehaviorSinkMap.values()) {
                ps.setString(1, entity.getModelingParamId());
                ps.setString(2, entity.getSrcId());
                ps.setString(3, entity.getSrcIp());
                ps.setString(4, entity.getDstIpSegment().toJSONString());
                ps.addBatch();
                hasData = true;
            }
            allAssetBehaviorSink.clear();
            if (hasData) {
                System.out.println("存入磁盘..........");
                ps.executeBatch();
            }
        }
    }


}
