package org.daijb.huat.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author daijb
 * @date 2021/3/2 19:07
 * 全局自定义线程池配置
 */
public class JavaThreadPoolConfigurer {

    private static final Logger logger = LoggerFactory.getLogger(JavaThreadPoolConfigurer.class);

    private static ScheduledThreadPoolExecutor taskScheduler;
    private static ThreadPoolExecutor asyncExecutor;

    public static ScheduledThreadPoolExecutor getTaskScheduler() {
        if (taskScheduler == null) {
            createThreadPools();
        }
        return taskScheduler;
    }

    public static ThreadPoolExecutor getAsyncExecutor() {
        if (taskScheduler == null) {
            createThreadPools();
        }
        return asyncExecutor;
    }

    private static void createThreadPools() {
        taskScheduler = new ScheduledThreadPoolExecutor(5, new DefaultThreadFactory("scheduler"));
        asyncExecutor = new ThreadPoolExecutor(10, 1000, 60, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(false), new DefaultThreadFactory("async"));
        asyncExecutor.allowCoreThreadTimeOut(true);

    }
}
