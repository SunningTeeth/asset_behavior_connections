package org.daijb.huat.services.entity;

import org.json.simple.JSONArray;

/**
 * @author daijb
 * @date 2021/2/18 12:52
 * CREATE TABLE `model_result_asset_behavior_relation`  (
 * `id` int(11) NOT NULL COMMENT 'ID',
 * `modeling_params_id` int(11) NULL DEFAULT NULL COMMENT '模型参数ID。根据模型参数ID可以知道建模的类型及子类型',
 * `src_id` varchar(100) CHARACTER SET utf8 COLLATE utf8_general_ci NULL DEFAULT NULL COMMENT '源资产ID',
 * `src_ip` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL COMMENT '资产源IP',
 * `dst_ip_segment` text CHARACTER SET utf8 COLLATE utf8_general_ci NULL COMMENT '目的IP网段。json格式，key 从1开始到 （模型结果时长/频率）例，一周每天的访问模型 结果为\r\n[    {\"name\": \"1\",\r\n   \"value\": \"IP1,IP2\"},\r\n    {\"name\": \"2\",\r\n   \"value\": \"IP1,IP2\"},\r\n    {\"name\": \"3\",\r\n   \"value\": \"IP1,IP2\"},\r\n    {\"name\": \"4\",\r\n   \"value\": \"IP1,IP2\"},\r\n    {\"name\": \"5\",\r\n   \"value\": \"IP1,IP2\"}\r\n]\r\n。\r\n\r\nvalue多个时候用 英文逗号 \',\' 隔开',
 * `time` datetime(0) NULL DEFAULT NULL COMMENT '数据插入时间',
 * PRIMARY KEY (`id`) USING BTREE
 * ) ENGINE = InnoDB CHARACTER SET = utf8 COLLATE = utf8_general_ci ROW_FORMAT = Dynamic;
 */
public class AssetBehaviorSink {

    private String modelingParamId;
    /**
     * 资产id
     */
    private String srcId;
    private String srcIp;
    private JSONArray dstIpSegment;

    public AssetBehaviorSink() {
    }

    public AssetBehaviorSink(String modelingParamId, String srcId, String srcIp, JSONArray dstIpSegment) {
        this.modelingParamId = modelingParamId;
        this.srcId = srcId;
        this.srcIp = srcIp;
        this.dstIpSegment = dstIpSegment;
    }

    public String getModelingParamId() {
        return modelingParamId;
    }

    public void setModelingParamId(String modelingParamId) {
        this.modelingParamId = modelingParamId;
    }

    public String getSrcId() {
        return srcId;
    }

    public void setSrcId(String srcId) {
        this.srcId = srcId;
    }

    public String getSrcIp() {
        return srcIp;
    }

    public void setSrcIp(String srcIp) {
        this.srcIp = srcIp;
    }

    public JSONArray getDstIpSegment() {
        return dstIpSegment;
    }

    public void setDstIpSegment(JSONArray dstIpSegment) {
        this.dstIpSegment = dstIpSegment;
    }

    @Override
    public String toString() {
        return "AssetBehaviorSink{" +
                "modelingParamId='" + modelingParamId + '\'' +
                ", srcId='" + srcId + '\'' +
                ", srcIp='" + srcIp + '\'' +
                ", dstIpSegment=" + dstIpSegment +
                '}';
    }
}
