package org.daijb.huat.services;

/**
 * @author daijb
 * @date 2021/2/18 17:07
 */
public enum ServiceState {

    /**
     * 启动中, 不可工作
     */
    Starting,
    /**
     * 启动完毕, 正常工作
     */
    Ready,
    /**
     * 已关闭
     */
    Stopped
}
