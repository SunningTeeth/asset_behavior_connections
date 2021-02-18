package org.daijb.huat.services.entity;

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

    public FlowEntity() {
    }

    public FlowEntity(String srcId, String srcIp, String dstId, String dstIP, long rTime, Integer areaId, String flowId) {
        this.srcId = srcId;
        this.srcIp = srcIp;
        this.dstId = dstId;
        this.dstIp = dstIP;
        this.rTime = rTime;
        this.areaId = areaId;
        this.flowId = flowId;
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
