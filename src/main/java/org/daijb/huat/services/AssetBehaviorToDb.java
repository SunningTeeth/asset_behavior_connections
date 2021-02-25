package org.daijb.huat.services;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.daijb.huat.services.entity.AssetBehaviorSink;
import org.daijb.huat.services.utils.ConversionUtil;
import org.daijb.huat.services.utils.DBConnectUtil;
import org.daijb.huat.services.utils.StringUtil;
import org.json.simple.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.daijb.huat.services.AssetBehaviorConstants.MODEL_ID;

/**
 * @author daijb
 * @date 2021/2/23 10:49
 */
public class AssetBehaviorToDb extends RichSinkFunction<JSONObject> {

    //private static final long serialVersionUID = 3334654984018091675L;

    private static final Logger logger = LoggerFactory.getLogger(AssetBehaviorToDb.class);

    private volatile long lastUpdateTime = 0;
    private volatile boolean isFirst = true;

    private PreparedStatement ps;
    private Connection connection;

    /**
     * 更新间隔
     */
    public final long INTERVAL = 60 * 60 * 1000;

    private ConcurrentMap<String, AssetBehaviorSink> allAssetBehaviorSink = new ConcurrentHashMap<>();

    public AssetBehaviorToDb() {
        if (isFirst) {
            String buildModelRate = AssetBehaviorBuildModelUtil.getBuildModelRate().trim();
            String unit = buildModelRate.substring(buildModelRate.length() - 2);
            int rate = ConversionUtil.toInteger(buildModelRate.substring(0, buildModelRate.length() - 2));
            TimeUnit timeUnit = TimeUnit.HOURS;
            if (StringUtil.equals(unit, "hh")) {
                timeUnit = TimeUnit.DAYS;
            }
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
        Executors.newScheduledThreadPool(3).scheduleWithFixedDelay(() -> {
            isFirst = false;
            try {
                startPrepareData();
            } catch (Throwable throwable) {
                logger.error("exec sql failed.", throwable);
            }
        }, 0, delay, timeUnit);
    }

    private void startPrepareData() throws Exception {
        final Map<String, AssetBehaviorSink> assetBehaviorSinkMap = allAssetBehaviorSink;
        if (allAssetBehaviorSink.size() > 4096 || lastUpdateTime < System.currentTimeMillis() - INTERVAL) {
            lastUpdateTime = System.currentTimeMillis();
            open();
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
                ps.executeBatch();
            }
        }
    }

    private void open() throws Exception {
        connection = DBConnectUtil.getConnection();
        String sql = "insert into `model_result_asset_behavior_relation` (`modeling_params_id`,`src_id`,`src_ip`,`dst_ip_segment`,`time`) values (?,?,?,?,?)";
        ps = connection.prepareStatement(sql);
    }
}

