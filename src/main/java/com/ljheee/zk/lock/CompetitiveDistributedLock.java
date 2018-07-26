package com.ljheee.zk.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 竞争式 分布死后锁
 */
public class CompetitiveDistributedLock {

    private ZooKeeper zookeeper;
    private CountDownLatch countDownLatch = new CountDownLatch(1);


    // 当前会话关联的最后一级znode，关联后，在当前会话过程中，将能够访问该节点以及其子节点，而不能访问该节点的上级节点和旁系节点，该节点可以视作是当前会话的根节点。
    private String connectString = "192.168.1.200:2181/myTest";


    private static String PARENT_PATH = "dlocks";

    public CompetitiveDistributedLock() {

        try {
            zookeeper = new ZooKeeper(connectString, 10000, new Watcher() {
                public void process(WatchedEvent watchedEvent) {
                    if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                        countDownLatch.countDown();
                    }
                }
            });
            countDownLatch.await();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }


    public void getLock() throws KeeperException, InterruptedException {
        Stat exists = zookeeper.exists(PARENT_PATH, false);
        if (exists == null) {
            // 创建分布式锁 持久化跟节点
            zookeeper.create(PARENT_PATH, "This is distribute lock parent path.".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        // 创建临时节点
        String lockPath = PARENT_PATH + "/" + "itemId";

        int tryCount = 20; // 尝试20次
        int tryTime = 0;    //
        int tryDelay = 2000; //尝试间隔事件

        try {
            // 创建临时节点，随会话存在
            zookeeper.create(lockPath, "child node.".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            while (tryTime < tryCount) {
                Thread.sleep(tryDelay);
                tryTime++;
                try {
                    zookeeper.create(lockPath, "child node.".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                } catch (Exception e1) {
                    continue;
                }
                break;
            }
        }
    }

    public void releaseLock() throws KeeperException, InterruptedException {
        zookeeper.delete(PARENT_PATH + "/" + "itemId", -1);
    }


    /**
     * 可以 开启多个客户端（进程），使用该分布式锁
     *
     * @param args
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws KeeperException, InterruptedException {
        CompetitiveDistributedLock distributedLock = new CompetitiveDistributedLock();

        distributedLock.getLock();

        //deal data
        Thread.sleep(10000);
        distributedLock.releaseLock();

    }


}
