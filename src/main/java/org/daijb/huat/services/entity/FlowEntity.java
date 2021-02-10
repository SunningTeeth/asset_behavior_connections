package org.daijb.huat.services.entity;

/**
 * @author daijb
 * @date 2021/2/10 17:16
 */
public class FlowEntity {

    private String srcId;
    private String srcIp;
    private String dstId;
    private String dstIP;
    private long atTimestamp;
    private Integer areaId;
    private String flowId;

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

    public String getDstIP() {
        return dstIP;
    }

    public void setDstIP(String dstIP) {
        this.dstIP = dstIP;
    }

    public long getAtTimestamp() {
        return atTimestamp;
    }

    public void setAtTimestamp(long atTimestamp) {
        this.atTimestamp = atTimestamp;
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
}
