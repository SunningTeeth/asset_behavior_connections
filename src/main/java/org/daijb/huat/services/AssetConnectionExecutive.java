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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author daijb
 * @date 2021/2/18 14:37
 */
public class AssetConnectionExecutive implements AssetBehaviorConstants, Serializable {

    private static final Logger logger = LoggerFactory.getLogger(AssetConnectionExecutive.class);

    private static volatile ServiceState state = ServiceState.Starting;

    /**
     * key : 资产id
     * value : sink
     */
    private final Map<String, AssetBehaviorSink> allAssetBehaviorSink = new ConcurrentHashMap<>();

    /**
     * 需要定时更新
     * key:区域id
     * value: HashMap == > key：资产id  value：资产ip
     */
    private Map<Integer, Map<String, Set<String>>> allAssetIps = new ConcurrentHashMap<>();

    private volatile Map<String, Object> modelingParams;

    private Lock lock = new ReentrantLock();

    private volatile long lastUpdateTime = 0;

    private volatile int currSegmentKey = 0;

    private volatile boolean isFirst = true;

    public AssetConnectionExecutive() {
        modelingParams = AssetBehaviorBuildModelUtil.getModelingParams();
        startThreadFunc();
    }

    public boolean assetBehaviorFilter(FlowEntity entity) throws Exception {
        checkState();
        if (state != ServiceState.Ready) {
            return false;
        }
        boolean result = false;
        if (allAssetIps.isEmpty() || lastUpdateTime < System.currentTimeMillis() - INTERVAL) {
            lastUpdateTime = System.currentTimeMillis();
            searchGroupIdAtIps();
        }
        String srcIp = ConversionUtil.toString(entity.getSrcIp());
        String dstIp = ConversionUtil.toString(entity.getDstIp());
        Integer areaId = entity.getAreaId();
        Map<String, Set<String>> stringSetMap = allAssetIps.get(areaId);
        if (stringSetMap == null) {
            return false;
        }
        while (!lock.tryLock()) {
        }
        try {
            for (Map.Entry<String, Set<String>> entry : stringSetMap.entrySet()) {
                Set<String> ips = entry.getValue();
                if (ips.contains(srcIp)) {
                    createEntityInfo(entry.getKey(), srcIp);
                    result = true;
                    continue;
                }
                if (ips.contains(dstIp)) {
                    createEntityInfo(entry.getKey(), dstIp);
                    result = true;
                }

            }
        } finally {
            lock.unlock();
        }

        return result;
    }

    public static final String PERSISTENCE_FILEPATH = "/usr/csp/model/assetBehavior/";

    private int calculateSegmentCurrKey() {
        //File file = new File(PERSISTENCE_FILEPATH);
        String buildModelRate = AssetBehaviorBuildModelUtil.getBuildModelRate();
        String rateUnit = buildModelRate.substring(buildModelRate.length() - 2);
        File file = new File("C:\\daijb\\demop\\");
        if (!file.exists()) {
            file.mkdirs();
        }
        int maxSegmentKey = calculateSegmentMaxKey();
        // 每种频率的开头的文件只有一个
        File[] childrenFiles = file.listFiles();
        for (File f : childrenFiles) {
            // 存在当前key  eg: ss_1
            String name = f.getName();
            if (name.contains(rateUnit)) {
                int ck = ConversionUtil.toInteger(name.substring(name.indexOf("_") + 1));
                currSegmentKey = ck + 1;
                if (currSegmentKey > maxSegmentKey) {
                    break;
                }
                f.renameTo(new File(f.getParentFile().getAbsolutePath() + File.separator + rateUnit + "_" + currSegmentKey));
            } else {
                //createNewFile
                File dest = new File(f.getParentFile().getAbsolutePath() + File.separator + rateUnit + "_1");
                try {
                    dest.createNewFile();
                } catch (IOException ignored) {
                }
            }
        }
        if (childrenFiles.length == 0) {
            File dest = new File("C:\\daijb\\demop\\" + rateUnit + "_1");
            try {
                dest.createNewFile();
            } catch (IOException ignored) {
            }
        }
        final int currSegmentKey0 = this.currSegmentKey;
        return currSegmentKey0;
    }

    private int calculateSegmentMaxKey() {
        AssetBehaviorBuildModelUtil.ModelCycle modelCycle = AssetBehaviorBuildModelUtil.getModelCycle();
        String buildModelRate = AssetBehaviorBuildModelUtil.getBuildModelRate();
        String rateNum = buildModelRate.substring(0, buildModelRate.length() - 2);
        String rateUnit = buildModelRate.substring(buildModelRate.length() - 2);
        int keyMax = 0;
        switch (modelCycle) {
            case DAYS: {
                // 建模周期为天时，建模频率暂为小时
                keyMax = 24 / ConversionUtil.toInteger(rateNum);
                break;
            }
            case WEEK: {
                //建模周期为周的时，建模频率只能是天
                keyMax = 7 / ConversionUtil.toInteger(rateNum);
                break;
            }
        }
        return keyMax;
    }

    /**
     * 检查模型开关
     */
    private void checkState() {
        if (ConversionUtil.toBoolean(modelingParams.get(MODEL_SWITCH))) {
            state = ServiceState.Ready;
        } else {
            state = ServiceState.Stopped;
            String updateSql = "UPDATE `modeling_params` SET `model_task_status`=?, `modify_time`=? WHERE (`id`='" + modelingParams.get(MODEL_ID) + "');";
            // 更新状态
            DBConnectUtil.execUpdateTask(updateSql, ModelStatus.FAILED.toString().toLowerCase(), LocalDateTime.now().toString());
        }
    }

    private void createEntityInfo(String assetId, String assetIp) {
        if (isFirst) {
            String updateSql = "UPDATE `modeling_params` SET `model_task_status`=?, `modify_time`=? WHERE (`id`='" + modelingParams.get(MODEL_ID) + "');";
            // 更新状态
            DBConnectUtil.execUpdateTask(updateSql, ModelStatus.SUCCESS.toString().toLowerCase(), LocalDateTime.now().toString());
        }
        AssetBehaviorSink entity = allAssetBehaviorSink.get(assetId);
        String modelId = modelingParams.get(MODEL_ID).toString();
        int currSegmentKey0 = this.currSegmentKey;
        if (entity == null) {
            JSONArray dstIpSegment = new JSONArray();
            com.alibaba.fastjson.JSONObject item = new com.alibaba.fastjson.JSONObject();
            item.put("name", currSegmentKey0);
            item.put("value", assetIp);
            dstIpSegment.add(item);
            allAssetBehaviorSink.put(assetId, new AssetBehaviorSink(modelId, assetId, assetIp, dstIpSegment));
        } else {
            // 更新(多久更新)
            JSONArray dstIpSegment = entity.getDstIpSegment();
            for (int i = 0; i < dstIpSegment.size(); i++) {
                com.alibaba.fastjson.JSONObject obj = (com.alibaba.fastjson.JSONObject) dstIpSegment.get(i);
                if (StringUtil.equals(ConversionUtil.toString(obj.get("name")), currSegmentKey0 + "")) {
                    obj.put("value", obj.get("value") + "," + assetIp);
                    break;
                }
            }
            entity.setDstIpSegment(dstIpSegment);
            allAssetBehaviorSink.put(assetId, entity);
        }

    }

    private synchronized void searchGroupIdAtIps() throws Exception {
        Map<Integer, Map<String, Set<String>>> allGroupIps0 = new HashMap<>();
        Connection connection = DBConnectUtil.getConnection();
        Statement statement = connection.createStatement();
        StringBuilder whereTxt = new StringBuilder();
        Object o = modelingParams.get(MODEL_ATTRS);
        JSONObject attrs = (JSONObject) JSONValue.parse(ConversionUtil.toString(o));
        Object modelEntityGroup = attrs.get("model_entity_group");
        if (modelEntityGroup == null) {
            logger.error("model entity group key is not exist");
            throw new Exception("model entity group key is not exist");
        }
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
        ResultSet resultSet = statement.executeQuery("SELECT * FROM asset WHERE entity_groups in " + whereTxt.toString());
        while (resultSet.next()) {
            String entityId = resultSet.getString("entity_id");
            String assetIp = resultSet.getString("asset_ip");
            Integer areaId = ConversionUtil.toInteger(resultSet.getString("area_id"));
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
        this.allAssetIps = allGroupIps0;
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

    public synchronized void startThreadFunc() {
        if (isFirst) {
            isFirst = false;
            String unit = modelingParams.get(MODEL_RATE_TIME_UNIT).toString();
            int rate = ConversionUtil.toInteger(modelingParams.get(MODEL_RATE_TIME_UNIT_NUM));
            TimeUnit timeUnit = TimeUnit.HOURS;
            if (StringUtil.equals(unit, "dd")) {
                timeUnit = TimeUnit.DAYS;
            }
            logger.info("开始定时任务.....");
            startTimerFunc(rate, timeUnit);
        }
    }

    private void startTimerFunc(int delay, TimeUnit timeUnit) {
        System.out.println(LocalDateTime.now().toString() + " 定时任务启动 delay : " + delay + ", timeUnit " + timeUnit);

        Timer timer = new Timer();
        Calendar currentTime = Calendar.getInstance();
        currentTime.setTime(new Date());
        int delay0 = 5 - currentTime.get(Calendar.MINUTE) % 5;
        currentTime.set(Calendar.MINUTE, currentTime.get(Calendar.MINUTE) + delay0);
        currentTime.set(Calendar.SECOND, 0);
        currentTime.set(Calendar.MILLISECOND, 0);

        Date firstTime = currentTime.getTime();
        // 每五分钟执行
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    checkModelInfo();
                } catch (Throwable throwable) {
                    logger.error("update model params task timer schedule failed ", throwable);
                }
            }
        }, firstTime, 1000 * 60 * 5);

        // 每五分钟执行
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                try {
                    calculateSegmentCurrKey();
                    startPrepareData();
                } catch (Throwable throwable) {
                    logger.error("错误 ： " + throwable);
                }
            }
        }, 1, 1000 * 60);
    }

    private void checkModelInfo() {
        Map<String, Object> newModelingParams = AssetBehaviorBuildModelUtil.buildModelingParams();
        modelParamsChange(newModelingParams);
        modelUpdateChange(newModelingParams);
    }

    /**
     * 更新方式是否发生变化
     */
    private boolean modelUpdateChange(Map<String, Object> newModelingParams) {

        // 更新方式
        Integer newModelUpdate = ConversionUtil.toInteger(newModelingParams.get(MODEL_UPDATE));
        Integer oldModelUpdate = ConversionUtil.toInteger(modelingParams.get(MODEL_UPDATE));
        if (newModelUpdate != null && !newModelUpdate.equals(oldModelUpdate)) {
            return true;
        }
        return false;
    }

    /**
     * 定期检查建模维度是否发生变化
     * 当前维度的定义： 建模周期 & 模型建模的频率 & 建模频率时间单位对应的数值
     */
    private boolean modelParamsChange(Map<String, Object> newModelingParams) {

        Map<String, Object> modelingParams = this.modelingParams;
        // 建模周期 model_result_span
        boolean modelResultSpanFlag = false;
        Integer newModelResultSpan = ConversionUtil.toInteger(newModelingParams.get(MODEL_RESULT_SPAN));
        Integer oldModelResultSpan = ConversionUtil.toInteger(modelingParams.get(MODEL_RESULT_SPAN));
        if (newModelResultSpan != null && !newModelResultSpan.equals(oldModelResultSpan)) {
            modelResultSpanFlag = true;
        }
FASDFASDFSDFASD
        // 模型建模的频率 model_rate_timeunit
        boolean modelRateTimeUnitFlag = false;
        String newModelRateTimeUnit = ConversionUtil.toString(newModelingParams.get(MODEL_RATE_TIME_UNIT));
        String oldModelRateTimeUnit = ConversionUtil.toString(modelingParams.get(MODEL_RATE_TIME_UNIT));
        if (StringUtil.equals(newModelRateTimeUnit, oldModelRateTimeUnit)) {
            modelRateTimeUnitFlag = true;
        }

        // 建模频率时间单位对应的数值 model_rate_timeunit_num
        boolean modelRateTimeUnitNumFlag = false;
        Integer newModelRateTimeUnitNum = ConversionUtil.toInteger(newModelingParams.get(MODEL_RATE_TIME_UNIT_NUM));
        Integer oldModelRateTimeUnitNum = ConversionUtil.toInteger(modelingParams.get(MODEL_RATE_TIME_UNIT_NUM));
        if (newModelRateTimeUnitNum != null && !newModelRateTimeUnitNum.equals(oldModelRateTimeUnitNum)) {
            modelRateTimeUnitNumFlag = true;
        }

        if (modelResultSpanFlag && modelRateTimeUnitFlag && modelRateTimeUnitNumFlag) {
            return true;
        }

        // 所需历史天数 model_history_data_span
        /*Integer newModelHistoryData = ConversionUtil.toInteger(newModelingParams.get(MODEL_HISTORY_DATA_SPAN));
        Integer oldModelHistoryData = ConversionUtil.toInteger(modelingParams.get(MODEL_HISTORY_DATA_SPAN));
        if (newModelHistoryData != null && !newModelHistoryData.equals(oldModelHistoryData)) {
            return true;
        }*/

        return false;
    }

    private void startPrepareData() throws Exception {
        final Map<String, AssetBehaviorSink> assetBehaviorSinkMap = allAssetBehaviorSink;
        logger.info("准备数据 , size : " + assetBehaviorSinkMap.size());
        if (assetBehaviorSinkMap.size() > 1 || lastUpdateTime < System.currentTimeMillis() - INTERVAL) {
            Connection connection = DBConnectUtil.getConnection();

            // 需要先查询是否有该模型记录


            String sql = "insert into `model_result_asset_behavior_relation` (`modeling_params_id`,`src_id`,`src_ip`,`dst_ip_segment`,`time`) values (?,?,?,?,?)";
            PreparedStatement ps = connection.prepareStatement(sql);
            lastUpdateTime = System.currentTimeMillis();
            boolean hasData = false;
            for (AssetBehaviorSink entity : assetBehaviorSinkMap.values()) {
                ps.setString(1, entity.getModelingParamId());
                ps.setString(2, entity.getSrcId());
                ps.setString(3, entity.getSrcIp());
                ps.setString(4, entity.getDstIpSegment().toJSONString());
                ps.setString(5, LocalDateTime.now().toString());
                ps.execute();
                hasData = true;
            }
            //connection.commit();
            if (hasData) {
                //ps.executeBatch();
                allAssetBehaviorSink.clear();
            }
        }
    }
}
