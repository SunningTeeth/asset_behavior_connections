package org.daijb.huat.streaming;

import org.daijb.huat.entity.AssetBehaviorSink;
import org.daijb.huat.utils.ConversionUtil;
import org.daijb.huat.utils.DbConnectUtil;
import org.daijb.huat.utils.StringUtil;
import org.json.simple.JSONArray;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * @author daijb
 * @date 2021/3/2 14:25
 * 将建模结果存储到数据库
 */
public class AssetBehaviorToDatabase {

    private static final Logger logger = LoggerFactory.getLogger(AssetBehaviorToDatabase.class);

    /**
     * 数据入库
     *
     * @param assetBehaviorSinkMap 数据源
     * @param modelingParams0      建模参数
     * @param isUpdate             更新方式：0(false)清除历史数据更新,1(true)累计迭代历史数据更新。
     * @return
     * @throws Exception
     */
    public static boolean save(Map<String, AssetBehaviorSink> assetBehaviorSinkMap, Map<String, Object> modelingParams0, boolean isUpdate) throws Exception {
        Connection connection = DbConnectUtil.getConnection();

        // 双map
        // key : 模型id
        // val : key==> 资产id ,value===>dstIpSegment
        Map<String, Map<String, JSONArray>> cacheModelResult = new HashMap<>();

        //当前模型id
        Object modelId = modelingParams0.get("modelId");

        // 迭代历史更新
        if (isUpdate) {

            // 需要先查询是否有该模型记录,进而进行归并
            if (modelId != null) {
                String querySql = "select `modeling_params_id`,`src_id`,`src_ip`,`dst_ip_segment`,`time` from model_result_asset_behavior_relation ;";

                PreparedStatement queryPs = connection.prepareStatement(querySql);
                ResultSet resultSet = queryPs.executeQuery();
                while (resultSet.next()) {
                    String modelingParamsId = resultSet.getString("modeling_params_id");
                    // 查到关联记录,需要合并数据
                    if (StringUtil.equals(modelingParamsId, modelId.toString())) {
                        Map<String, JSONArray> valMap = new HashMap<>();
                        JSONArray segmentArr = (JSONArray) JSONValue.parse(resultSet.getString("dst_ip_segment"));
                        String srcId = ConversionUtil.toString(resultSet.getString("src_id"));
                        valMap.put(srcId, segmentArr);
                        cacheModelResult.put(modelingParamsId, valMap);
                    }
                }
            }

        } else {
            // 0(false)清除历史数据更新
            String deleteSql = "DELETE FROM model_result_asset_behavior_relation WHERE modeling_params_id ='" + modelId.toString() + "';";
            boolean result = connection.createStatement().execute(deleteSql);
            logger.info("remove model_result_asset_behavior_relation data of modeling_params_id : " + modelId + ", result :" + result);
        }

        boolean hasData = false;
        if (cacheModelResult.isEmpty()) {
            if (isUpdate) {
                return false;
            }
            // 新增数据入库
            String updateSql = "insert into `model_result_asset_behavior_relation` (`modeling_params_id`,`src_id`,`src_ip`,`dst_ip_segment`,`time`) values (?,?,?,?,?)";
            connection.setAutoCommit(false);
            PreparedStatement ps = connection.prepareStatement(updateSql);
            for (AssetBehaviorSink entity : assetBehaviorSinkMap.values()) {
                ps.setString(1, entity.getModelingParamId());
                ps.setString(2, entity.getSrcId());
                ps.setString(3, entity.getSrcIp());
                ps.setString(4, entity.getDstIpSegment().toJSONString());
                ps.setString(5, LocalDateTime.now().toString());
                ps.addBatch();
                hasData = true;
            }
            if (hasData) {
                logger.info("model_result_asset_behavior_relation insert data : " + assetBehaviorSinkMap.size());
                DbConnectUtil.commit(connection);
            }
        } else {

            // 合并数据
            for (AssetBehaviorSink entity : assetBehaviorSinkMap.values()) {
                Map<String, JSONArray> stringJSONArrayMap = cacheModelResult.get(entity.getModelingParamId());
                if (stringJSONArrayMap == null || stringJSONArrayMap.isEmpty()) {
                    continue;
                }
                JSONArray segmentArr = stringJSONArrayMap.get(entity.getSrcId());
                if (segmentArr == null || segmentArr.isEmpty()) {
                    continue;
                }
                entity.setDstIpSegment(segmentArr);
            }

            String updateSql = "update model_result_asset_behavior_relation set dst_ip_segment =?,time=? where modeling_params_id=? and src_id=?";
            // 更新数据
            connection.setAutoCommit(false);
            PreparedStatement ps = connection.prepareStatement(updateSql);
            for (AssetBehaviorSink entity : assetBehaviorSinkMap.values()) {
                ps.setString(1, entity.getDstIpSegment().toJSONString());
                ps.setString(2, LocalDateTime.now().toString());
                ps.setString(3, entity.getModelingParamId());
                ps.setString(4, entity.getSrcId());
                ps.addBatch();
                hasData = true;
            }
            if (hasData) {
                DbConnectUtil.commit(connection);
            }
            logger.info("model_result_asset_behavior_relation update data : " + assetBehaviorSinkMap.size());

        }
        return hasData;
    }
}

