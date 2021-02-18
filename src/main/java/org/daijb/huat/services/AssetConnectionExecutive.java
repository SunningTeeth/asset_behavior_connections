package org.daijb.huat.services;

import org.daijb.huat.services.entity.AssetBehaviorSink;
import org.daijb.huat.services.entity.FlowEntity;
import org.daijb.huat.services.utils.ConversionUtil;
import org.daijb.huat.services.utils.DBConnectUtil;
import org.daijb.huat.services.utils.StringUtil;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author daijb
 * @date 2021/2/18 14:37
 */
public class AssetConnectionExecutive implements AssetBehaviorConstants, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(AssetConnectionExecutive.class);

    public AssetConnectionExecutive() {
        modelingParams = AssetBehaviorBuildModelUtil.getModelingParams();
        if (ConversionUtil.toBoolean(modelingParams.get(MODEL_SWITCH))) {
            state = ServiceState.Ready;
        }
    }

    private static volatile ServiceState state = ServiceState.Starting;

    private Map<String, Object> modelingParams;

    /**
     * key : 资产id
     * value : sink
     */
    private ConcurrentMap<String, AssetBehaviorSink> allAssetBehaviorSink = new ConcurrentHashMap<>();

    /**
     * 需要定时更新
     * key:区域id
     * value: HashMap == > key：groupId  value：该自定义组内所有ip
     */
    private Map<Integer, Map<String, Set<String>>> allGroupsIps = new HashMap<>();

    private volatile long lastUpdateTime = 0;

    public boolean assetBehaviorFilter(FlowEntity entity) throws Exception {
        if (state != ServiceState.Ready) {
            return false;
        }
        if (modelingParams == null) {
            modelingParams = AssetBehaviorBuildModelUtil.getModelingParams();
        }
        boolean result = false;
        if (allGroupsIps.isEmpty() || lastUpdateTime < System.currentTimeMillis() - INTERVAL) {
            lastUpdateTime = System.currentTimeMillis();
            searchGroupIdAtIps();
        }
        String srcIp = ConversionUtil.toString(entity.getSrcIp());
        String dstIp = ConversionUtil.toString(entity.getDstIp());
        Integer areaId = entity.getAreaId();
        Map<String, Set<String>> stringSetMap = allGroupsIps.get(areaId);
        if (stringSetMap == null) {
            return false;
        }
        boolean isIgnoreSrcIp = true, isIgnoreDstIp = true;
        Set<String> ignoreIp = new HashSet<>();
        for (Map.Entry<String, Set<String>> entry : stringSetMap.entrySet()) {
            Set<String> ips = entry.getValue();
            JSONArray segment = new JSONArray();
            if (ips.contains(srcIp)) {
                isIgnoreSrcIp = false;
                JSONObject json = new JSONObject();
                int key = AssetBehaviorBuildModelUtil.calculateSegmentKey();
                if (key <= 0) {
                    continue;
                }
                json.put("name", key);
                json.put("value", srcIp);
                segment.add(json);
                createEntityInfo(entry.getKey(), srcIp, segment);
                result = true;
            }
            if (ips.contains(dstIp)) {
                isIgnoreDstIp = false;
                JSONObject json = new JSONObject();
                int key = AssetBehaviorBuildModelUtil.calculateSegmentKey();
                if (key <= 0) {
                    continue;
                }
                json.put("name", key);
                json.put("value", dstIp);
                segment.add(json);
                createEntityInfo(entry.getKey(), dstIp, segment);
                result = true;
            }
            if (isIgnoreDstIp && isIgnoreSrcIp) {
                ignoreIp.add(srcIp);
                ignoreIp.add(dstIp);
            }
        }
        logger.info("asset behavior connection ignore ips : " + ignoreIp);
        return result;
    }

    private void createEntityInfo(String srcId, String srcIp, JSONArray dstIpSegment0) {
        if (modelingParams == null) {
            modelingParams = AssetBehaviorBuildModelUtil.getModelingParams();
        }
        AssetBehaviorSink entity = allAssetBehaviorSink.get(srcId);
        if (entity == null) {
            allAssetBehaviorSink.put(srcId, new AssetBehaviorSink(modelingParams.get(MODEL_ID).toString(), srcId, srcIp, dstIpSegment0));
        } else {
            // 更新(多久更新)
            JSONArray dstIpSegment = entity.getDstIpSegment();
            dstIpSegment.addAll(dstIpSegment0);
            allAssetBehaviorSink.put(srcId, new AssetBehaviorSink(modelingParams.get(MODEL_ID).toString(), srcId, srcIp, dstIpSegment));
        }

    }

    private synchronized void searchGroupIdAtIps() throws Exception {
        Map<Integer, Map<String, Set<String>>> allGroupIps0 = new HashMap<>();
        Connection connection = DBConnectUtil.getConnection();
        Statement statement = connection.createStatement();
        StringBuilder whereTxt = new StringBuilder();
        if (modelingParams == null) {
            modelingParams = AssetBehaviorBuildModelUtil.getModelingParams();
        }
        Object o = modelingParams.get(MODEL_ATTRS);
        JSONObject attrs = (JSONObject) JSONValue.parse(ConversionUtil.toString(o));
        Object modelEntityGroup = attrs.get("model_entity_group");
        if (modelEntityGroup == null) {
            logger.error("model entity group key is not exist");
            throw new Exception("model entity group key is not exist");
        }
        //grp_75Tb5NMJDoEzCB3JxXcYgF,grp_75Tb5NMJDoFj1xdjMg9mL8
        String[] groupIds = ConversionUtil.toString(modelEntityGroup).split(",");
        whereTxt.append("(");
        StringBuilder tempTxt = new StringBuilder();
        for (String groupId : groupIds) {
            tempTxt.append("'").append(groupId).append("'").append(",");
        }
        String temp = tempTxt.toString();
        if (temp.endsWith(",")) {
            temp = temp.substring(0, temp.length() - 1);
        }
        whereTxt.append(temp).append(");");
        /*if (whereTxt.length() <= 0) {
            logger.error("model entity group value is null");
            throw new Exception("model entity group value is nul");
        }*/
        ResultSet resultSet = statement.executeQuery("SELECT * FROM asset WHERE entity_groups in " + whereTxt.toString());
        while (resultSet.next()) {
            String entityId = resultSet.getString("entity_id");
            String assetIp = resultSet.getString("asset_ip");
            Integer areaId = ConversionUtil.toInteger(resultSet.getString("area_id"));
            //String groupId = resultSet.getString("entity_groups");
            if (allGroupIps0.get(areaId) == null) {
                Map<String, Set<String>> map = new HashMap<>();
                Set<String> ips = new HashSet<>(getAssetOfIps(assetIp));
                map.put(entityId, ips);
                allGroupIps0.put(areaId, map);
            } else {
                // 同一个区域多个组情况
                Map<String, Set<String>> map = allGroupIps0.get(areaId);
                Set<String> ips = map.get(entityId);
                if (ips == null) {
                    ips = new HashSet<>();
                }
                ips.addAll(getAssetOfIps(assetIp));
                map.put(entityId, ips);
                allGroupIps0.put(areaId, map);
            }
        }
        this.allGroupsIps = allGroupIps0;
    }

    private Set<String> getAssetOfIps(String assetIp) {
        //  {"00:00:00:00:00:00":["192.168.8.97"]}
        Set<String> ips = new HashSet<>();
        JSONObject json = (JSONObject) JSONValue.parse(assetIp);
        for (Object o : json.values()) {
            JSONArray arr = (JSONArray) JSONValue.parse(o.toString());
            for (Object oo : arr) {
                ips.add(ConversionUtil.toString(oo));
            }
        }
        return ips;
    }
}
