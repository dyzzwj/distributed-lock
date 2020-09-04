package com.dyzwj.distributed.inter;

/**
 * @author zhengwenjie
 * @version 1.0.0
 * @ClassName DistributedLock.java
 * @Description TODO
 * @createTime 2020年08月04日 16:10:00
 */
public interface DistributedLock {

    void lock() throws InterruptedException;

    void unlock();

    /**
     * 释放此分布式锁所需要的资源，比如zookeeper连接
     */
    void releaseResource();
}
