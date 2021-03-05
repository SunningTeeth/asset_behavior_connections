package org.daijb.huat.services;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.daijb.huat.config.ModelParamsConfigurer;
import org.daijb.huat.entity.AssetBehaviorSink;
import org.daijb.huat.utils.ConversionUtil;
import org.daijb.huat.utils.DbConnectUtil;
import org.daijb.huat.utils.StringUtil;
import org.daijb.huat.utils.UUIDUtil;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.*;

import static org.daijb.huat.AssetBehaviorConstants.MODEL_ID;
import static org.daijb.huat.AssetBehaviorConstants.MODEL_RESULT_SPAN;

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
        connection = DbConnectUtil.getConnection();
        String sql = "insert into `model_result_asset_behavior_relation` (`id`,`modeling_params_id`,`src_id`,`src_ip`,`dst_ip_segment`,`time`)" +
                " values (?,?,?,?,?,?) ON DUPLICATE KEY UPDATE `dst_ip_segment`=?";
        ps = connection.prepareStatement(sql);
        //startThreadFunc();
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
        String key = ConversionUtil.toString(calculateSegmentCurrKey());
        String modelId = ModelParamsConfigurer.getModelingParams().get("modelId").toString();
        String querySql = "select * from model_result_asset_behavior_relation where modeling_params_id='" + modelId + "';";
        PreparedStatement preparedStatement = connection.prepareStatement(querySql);
        ResultSet resultSet = preparedStatement.executeQuery();
        String oldSegmentStr = "";
        while (resultSet.next()) {
            oldSegmentStr = resultSet.getString("dst_ip_segment");
        }
        JSONArray segmentArr = null;
        String hostIp = json.get("hostIp").toString();
        if (!StringUtil.isEmpty(oldSegmentStr)) {
            JSONArray segmentArrTemp = (JSONArray) JSONValue.parse(oldSegmentStr);
            JSONArray temp = new JSONArray();
            for (Object obj : segmentArrTemp) {
                JSONObject item = (JSONObject) JSONValue.parse(obj.toString());
                Set set = new HashSet();
                if (item.get(key) != null) {
                    set.addAll((JSONArray) JSONValue.parse(item.get(key).toString()));
                }
                set.add(hostIp);
                JSONArray arr = new JSONArray();
                arr.addAll(set);
                item.put(key, arr);
                temp.add(item);
            }
            segmentArr = temp;
        } else {
            segmentArr = new JSONArray();
            JSONObject item = new JSONObject();
            JSONArray ips = new JSONArray();
            ips.add(hostIp);
            item.put(key, ips);
            segmentArr.add(item);
        }
        ps.setString(1, "mdl_" + UUIDUtil.genId());
        ps.setString(2, modelId);
        ps.setString(3, json.get("entityId").toString());
        ps.setString(4, getAssetOfIp(json.get("assetIp").toString()));
        ps.setString(5, segmentArr.toJSONString());
        ps.setString(6, LocalDateTime.now().toString());
        ps.setString(7, segmentArr.toJSONString());
        ps.execute();
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
        int cycle = ConversionUtil.toInteger(ModelParamsConfigurer.getModelingParams().get(MODEL_RESULT_SPAN));
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

    private String getAssetOfIp(String assetIp) {
        //  {"00:00:00:00:00:00":["192.168.8.97"]}
        int start = assetIp.indexOf("[") + 1;
        int end = assetIp.indexOf("]");
        return assetIp.substring(start, end);
    }


}
