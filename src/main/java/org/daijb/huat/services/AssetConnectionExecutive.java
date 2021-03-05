package org.daijb.huat.services;

import org.daijb.huat.AssetBehaviorConstants;
import org.daijb.huat.config.JavaThreadPoolConfigurer;
import org.daijb.huat.config.ModelParamsConfigurer;
import org.daijb.huat.entity.AssetBehaviorSink;
import org.daijb.huat.entity.FlowEntity;
import org.daijb.huat.streaming.AssetBehaviorToDatabase;
import org.daijb.huat.utils.ConversionUtil;
import org.daijb.huat.utils.DbConnectUtil;
import org.daijb.huat.utils.StringUtil;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.sql.Connection;
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

    private Lock matchAssetLock = new ReentrantLock();
    private Lock modelTaskStatusLock = new ReentrantLock();

    private volatile long lastUpdateTime = 0;

    private volatile boolean isFirst = true;

    public AssetConnectionExecutive() {
        modelingParams = ModelParamsConfigurer.getModelingParams();
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
        while (!matchAssetLock.tryLock()) {
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
            matchAssetLock.unlock();
        }

        return result;
    }

    /**
     * 建模结果周期：1 代表一天。
     * 2 代表一周。3 代表一季度。
     * 4 代表一年。如果总长度填写 1 ，建模的时间单位可以是 ss mm hh 。周的建模时间单位只能是 dd 。其他的只能为月
     * 计算模型的key
     * <pre>
     *   1. 建模周期为天 ：
     *       SegmentKey : 每次从当前日期开始计算,以小时为key
     *   2. 建模周期为周：建模时间单位只能是天）
     *       SegmentKey : 每次从当前日期开始计算,以当前周几为key
     *   3. 建模周期为季度：（建模时间单位只能是月）
     *      SegmentKey : 每次从当前日期开始计算,以当前月份开始递增
     *   4. 建模周期为年：（建模时间单位只能是月）
     *      SegmentKey : 每次从当前日期开始计算,以当前月份开始递增
     *
     * </pre>
     */
    private Object calculateSegmentCurrKey() throws Exception {

        // 建模周期
        int cycle = ConversionUtil.toInteger(this.modelingParams.get(MODEL_RESULT_SPAN));
        LocalDateTime now = LocalDateTime.now();
        Object segmentKey = null;
        switch (cycle) {
            // 暂时默认为小时
            case 1: {
                segmentKey = now.getHour();
                break;
            }
            //周,频率只能是 dd
            case 2: {
                segmentKey = now.getDayOfWeek().getValue();
                break;
            }
            //季度,频率只能是月
            case 3:
            case 4: {
                // 年,频率只能是月
                segmentKey = now.getMonth().getValue();
                break;
            }
            default: {
                throw new Exception("modeling span is not support.");
            }
        }
        return segmentKey;
    }

    /**
     * 创建实体对象
     *
     * @param assetId 资产id
     * @param assetIp 资产ip
     * @throws Exception 异常
     */
    private void createEntityInfo(String assetId, String assetIp) throws Exception {
        if (isFirst) {
            // 更新状态
            updateModelTaskStatus(ModelStatus.SUCCESS);
            //String updateSql = "UPDATE `modeling_params` SET `model_task_status`=?, `modify_time`=? WHERE (`id`='" + modelingParams.get(MODEL_ID) + "');";
            //DbConnectUtil.execUpdateTask(updateSql, ModelStatus.SUCCESS.toString().toLowerCase(), LocalDateTime.now().toString());
        }
        AssetBehaviorSink entity = allAssetBehaviorSink.get(assetId);
        String modelId = this.modelingParams.get(MODEL_ID).toString();
        Object currSegmentKey0 = calculateSegmentCurrKey();
        if (entity == null) {
            JSONArray dstIpSegment = new JSONArray();
            com.alibaba.fastjson.JSONObject item = new com.alibaba.fastjson.JSONObject();
            item.put("name", currSegmentKey0);
            Set<String> value = new HashSet<>();
            value.add(assetIp);
            item.put("value", value);
            dstIpSegment.add(item);
            allAssetBehaviorSink.put(assetId, new AssetBehaviorSink(modelId, assetId, assetIp, dstIpSegment));
        } else {
            // 更新(多久更新)
            JSONArray dstIpSegment = entity.getDstIpSegment();
            for (int i = 0; i < dstIpSegment.size(); i++) {
                com.alibaba.fastjson.JSONObject obj = (com.alibaba.fastjson.JSONObject) dstIpSegment.get(i);
                if (StringUtil.equals(ConversionUtil.toString(obj.get("name")), ConversionUtil.toString(currSegmentKey0))) {
                    Set<String> value = (Set<String>) obj.get("value");
                    value.add(assetIp);
                    obj.put("value", value);
                    break;
                }
            }
            entity.setDstIpSegment(dstIpSegment);
            allAssetBehaviorSink.put(assetId, entity);
        }

    }

    /**
     * 查询
     *
     * @throws Exception
     */
    private synchronized void searchGroupIdAtIps() throws Exception {
        Map<Integer, Map<String, Set<String>>> allGroupIps0 = new HashMap<>();
        StringBuilder whereTxt = new StringBuilder();
        Object o = this.modelingParams.get(MODEL_ATTRS);
        JSONObject attrs = (JSONObject) JSONValue.parse(ConversionUtil.toString(o));
        Object modelEntityGroup = attrs.get("model_entity_group");
        if (modelEntityGroup == null) {
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
        Connection connection = DbConnectUtil.getConnection();
        Statement statement = connection.createStatement();
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
        logger.info("modeling params group joint asset ip size : " + allGroupIps0.size() + ", data : " + allGroupIps0.toString());
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

    public void startThreadFunc() {
        if (isFirst) {
            isFirst = false;
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
                        checkModelInfo();
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
                        calculateSegmentCurrKey();
                        startPrepareData();
                    } catch (Throwable throwable) {
                        logger.error("asset behavior  schedule task failed : ", throwable);
                    }
                }
            }, 1000 * 20, 1000 * 60);
        }
    }

    /**
     * 检查模型各种信息
     */
    private void checkModelInfo() {
        Map<String, Object> newModelingParams = ModelParamsConfigurer.buildModelingParams();
        if (newModelingParams.isEmpty()) {
            return;
        }
        boolean updateChange = modelUpdateChange(newModelingParams);
        if (updateChange) {
            // 更新方式发生变化
            // 模型的更新方式:
            // 0(false)清除历史数据更新
            // 1(true)累计迭代历史数据更新。
            // 模型结果的唯一性:模型分类 & 模型子类 & 频率 & 频数
            // 更改 历史数据和置信度 更新。
            boolean modelUpdate = ConversionUtil.toBoolean(newModelingParams.get(MODEL_UPDATE));
            final Map<String, AssetBehaviorSink> assetBehaviorSinkMap = allAssetBehaviorSink;
            try {
                AssetBehaviorToDatabase.save(assetBehaviorSinkMap, newModelingParams, modelUpdate);
            } catch (Throwable throwable) {
                logger.error("model info[model_update :" + modelUpdate + " is changed] force flush data failed", throwable);
            }

        }

        //建模维度变化
        boolean modelParamsChange = modelParamsChange(newModelingParams);
        if (modelParamsChange) {

        }
    }

    /**
     * 检查模型开关
     */
    private void checkState() {
        if (ConversionUtil.toBoolean(this.modelingParams.get(MODEL_SWITCH))) {
            state = ServiceState.Ready;
        } else {
            state = ServiceState.Stopped;
            // 更新状态
            updateModelTaskStatus(ModelStatus.FAILED);
            //String updateSql = "UPDATE `modeling_params` SET `model_task_status`=?, `modify_time`=? WHERE (`id`='" + modelingParams.get(MODEL_ID) + "');";
            //DbConnectUtil.execUpdateTask(updateSql, ModelStatus.FAILED.toString().toLowerCase(), LocalDateTime.now().toString());
        }
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

    /**
     * 准备数据
     */
    private void startPrepareData() {
        logger.info("吼吼吼吼吼吼吼吼吼吼吼吼吼吼吼吼");
        final Map<String, AssetBehaviorSink> assetBehaviorSinkMap = this.allAssetBehaviorSink;
        final Map<String, Object> modelingParams0 = this.modelingParams;
        if (assetBehaviorSinkMap.size() > 10 || lastUpdateTime < System.currentTimeMillis() - INTERVAL) {
            lastUpdateTime = System.currentTimeMillis();
            try {
                logger.info("start save data of size : " + assetBehaviorSinkMap.size());
                boolean saveResult = AssetBehaviorToDatabase.save(assetBehaviorSinkMap, modelingParams0, true);
                if (saveResult) {
                    allAssetBehaviorSink.clear();
                }
            } catch (Throwable throwable) {
                logger.error("save data error ", throwable);
            }
        }
    }

    /**
     * 更新建模状态
     *
     * @param modelStatus 状态枚举
     */
    private void updateModelTaskStatus(ModelStatus modelStatus) {
        while (!modelTaskStatusLock.tryLock()) {
        }
        try {
            String updateSql = "UPDATE `modeling_params` SET `model_task_status`=?, `modify_time`=? WHERE (`id`='" + modelingParams.get(MODEL_ID) + "');";
            DbConnectUtil.execUpdateTask(updateSql, modelStatus.toString().toLowerCase(), LocalDateTime.now().toString());
            logger.info("update model task status : " + modelStatus.name());
        } finally {
            modelTaskStatusLock.unlock();
        }
    }
}
