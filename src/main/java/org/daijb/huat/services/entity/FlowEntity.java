package org.daijb.huat.services.entity;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

/**
 * @author daijb
 * @date 2021/2/10 17:16
 */
public class FlowEntity {

    private String srcId;
    private String srcIp;
    private String dstId;
    private String dstIp;
    private long rTime;
    private Integer areaId;
    private String flowId;

    //==========为方便sink====
    private String assetId;
    private String assetIp;
    private JSONArray dstIpSegment;
    private String modelingParamId;
    private String sTime;

    public FlowEntity() {
    }

    public FlowEntity(String assetId, String assetIp, JSONArray dstIpSegment, String modelingParamId) {
        this.assetId = assetId;
        this.assetIp = assetIp;
        this.dstIpSegment = dstIpSegment;
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

    public String getDstId() {
        return dstId;
    }

    public void setDstId(String dstId) {
        this.dstId = dstId;
    }

    public String getDstIp() {
        return dstIp;
    }

    public void setDstIp(String dstIp) {
        this.dstIp = dstIp;
    }

    public long getrTime() {
        return rTime;
    }

    public void setrTime(long rTime) {
        this.rTime = rTime;
    }

    public Integer getAreaId() {
        return areaId;
    }

    public void setAreaId(Integer areaId) {
        this.areaId = areaId;
    }

    public String getFlowId() {
        return flowId;
    }

    public void setFlowId(String flowId) {
        this.flowId = flowId;
    }

    public String getAssetId() {
        return assetId;
    }

    public void setAssetId(String assetId) {
        this.assetId = assetId;
    }

    public String getAssetIp() {
        return assetIp;
    }

    public void setAssetIp(String assetIp) {
        this.assetIp = assetIp;
    }

    public JSONArray getDstIpSegment() {
        return dstIpSegment;
    }

    public void setDstIpSegment(JSONArray dstIpSegment) {
        this.dstIpSegment = dstIpSegment;
    }

    public String getModelingParamId() {
        return modelingParamId;
    }

    public void setModelingParamId(String modelingParamId) {
        this.modelingParamId = modelingParamId;
    }

    public String getsTime() {
        return sTime;
    }

    public void setsTime(String sTime) {
        this.sTime = sTime;
    }

    public JSONObject toJSONObject() {
        JSONObject json = new JSONObject();
        json.put("SrcID", getSrcId());
        json.put("SrcIP", getSrcIp());
        json.put("DstID", getDstId());
        json.put("DstIP", getDstIp());
        json.put("AreaID", getAreaId());
        json.put("FlowID", getFlowId());
        json.put("rTime", getrTime());

        return json;
    }

    @Override
    public String toString() {
        return "FlowEntity{" +
                "srcId='" + srcId + '\'' +
                ", srcIp='" + srcIp + '\'' +
                ", dstId='" + dstId + '\'' +
                ", dstIp='" + dstIp + '\'' +
                ", rTime=" + rTime +
                ", areaId=" + areaId +
                ", flowId='" + flowId + '\'' +
                '}';
    }
}
