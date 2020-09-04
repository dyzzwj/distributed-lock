package com.dyzwj.distributed.lock;

import com.dyzwj.distributed.inter.DistributedLock;
import org.apache.zookeeper.*;


import java.io.IOException;
import java.nio.charset.StandardCharsets;

import java.util.UUID;

/**
 * @author zhengwenjie
 * @version 1.0.0
 * @ClassName NotReentrantDistributeLockWithHerdEffect.java
 * @Description TODO
 * @createTime 2020年08月04日 16:08:00
 *
 *  * 不可重入有羊群效应的分布式锁（不公平锁）
 *  * 只有一个线程会成功创建临时节点（拿到锁），其他线程会创建对这个节点的监听（等待），拿到锁的线程释放锁的时候（删除节点）
 *  * zk会回调对这个阶段注册监听的方法，这些线程都会去创建节点，最终只有一个线程会成功创建
 *  *
 *  * 缺点：即当有很多进程在等待锁的时候，在释放锁的时候会有很多进程就过来争夺锁，这种现象称为 “惊群效应”
 */
public class NotReentrantDistributeLockWithHerdEffect implements DistributedLock {


    private Thread currentThread;

    private String lockBasePath;
    private String lockName;
    private String lockFullPath;

    private ZooKeeper zooKeeper;
    private String myId;
    private String myName;

    private String zkQurom = "localhost:2182";

    public NotReentrantDistributeLockWithHerdEffect(String lockBasePath, String lockName) throws IOException {
        this.lockBasePath = lockBasePath;
        this.lockName = lockName;
        this.lockFullPath = this.lockBasePath + "/" + lockName;
        this.zooKeeper = new ZooKeeper(zkQurom, 6000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("Receive event "+watchedEvent);
                if(Event.KeeperState.SyncConnected == watchedEvent.getState())
                    System.out.println("connection is established...");
            }
        });
        this.currentThread = Thread.currentThread();
        this.myId = UUID.randomUUID().toString();
        this.myName = Thread.currentThread().getName();
        createLockBasePath();
    }

    private void createLockBasePath() {
        try {
            if (zooKeeper.exists(lockBasePath, null) == null) {
                zooKeeper.create(lockBasePath, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException | InterruptedException ignored) {

        }
    }

    @Override
    public void lock() throws InterruptedException {
        try {
            zooKeeper.create(lockFullPath, myId.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println(myName + ": get lock");
        } catch (KeeperException.NodeExistsException e) { //捕获节点已经存在异常
            // 如果节点已经存在，则监听此节点，当节点被删除时再抢占锁
            try {
                /**
                 * zk.exists(path,watcher):
                 *
                 返回给定路径的节点的统计信息。如果不存在这样的节点，则返回null。
                 如果watcher非空并且调用成功（未引发异常），则在给定路径的节点上注册一个watcher。
                 成功创建/删除节点或在节点上设置数据的成功操作将触发watcher。
                 */
                zooKeeper.exists(lockFullPath, event -> {
                    synchronized (currentThread) {
                        System.out.println(Thread.currentThread().getName() + "====>" + currentThread.getName());
                        currentThread.notify();
                        System.out.println(myName + ": wake");
                    }
                });
            } catch (KeeperException.NoNodeException e1) {
                // 间不容发之际，其它人释放了锁
                lock();
            } catch (KeeperException | InterruptedException e1) {
                e1.printStackTrace();
            }
            synchronized (currentThread) {
                //等待
                currentThread.wait();
            }
            //被唤醒之后继续尝试获取锁
            lock();
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void unlock() {
        try {
            byte[] nodeBytes = zooKeeper.getData(lockFullPath, false, null);
            String currentHoldLockNodeId = new String(nodeBytes, StandardCharsets.UTF_8);
            // 只有当前锁的持有者是自己的时候，才能删除节点
            if (myId.equalsIgnoreCase(currentHoldLockNodeId)) {
                zooKeeper.delete(lockFullPath, -1);
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        System.out.println(myName + ": releaseResource lock");
    }

    @Override
    public void releaseResource() {
        try {
            // 将zookeeper连接释放掉
            zooKeeper.close();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}



