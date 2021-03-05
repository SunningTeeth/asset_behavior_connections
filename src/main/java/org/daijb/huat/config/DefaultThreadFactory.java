package org.daijb.huat.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author daijb
 * @date 2021/3/2 19:09
 */
public class DefaultThreadFactory implements ThreadFactory {

    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final int priority;
    private final String poolName;

    public DefaultThreadFactory(String poolName) {
        this(poolName, Thread.NORM_PRIORITY);
    }

    public DefaultThreadFactory(String poolName, int priority) {
        this.priority = priority;
        this.poolName = poolName;
        SecurityManager s = System.getSecurityManager();
        group = new PoolThreadGroup(poolName);
    }

    @Override
    public Thread newThread(Runnable r) {
        Thread t = new Thread(group, r, poolName + "-" + threadNumber.getAndIncrement(), 0);
        t.setDaemon(true);
        t.setPriority(priority);
        return t;
    }

    public static class PoolThreadGroup extends ThreadGroup {

        private static final Logger logger = LoggerFactory.getLogger(PoolThreadGroup.class);

        public PoolThreadGroup(String name) {
            super(name);
        }

        @Override
        public void uncaughtException(Thread t, Throwable e) {
            logger.error("Thread " + t + " uncaught exception: " + e, e);
        }

    }
}