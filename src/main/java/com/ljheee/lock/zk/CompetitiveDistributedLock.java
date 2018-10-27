package com.ljheee.lock.zk;

import com.ljheee.lock.IDistributedLock;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * 竞争式 分布式锁
 */
public class CompetitiveDistributedLock implements IDistributedLock {

    private ZooKeeper zookeeper;
    private CountDownLatch countDownLatch = new CountDownLatch(1);


    // 当前会话关联的最后一级znode，关联后，在当前会话过程中，将能够访问该节点以及其子节点，而不能访问该节点的上级节点和旁系节点，该节点可以视作是当前会话的根节点。
    // myTest 是要连接的 永久节点
    private String connectString = "127.0.0.1:2181/myTest";


    // Path must start with / character
    private static String PARENT_PATH = "/dlocks";

    /**
     * 构造方法
     * 完成zk数据源的连接
     */
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


    /**
     * 获得锁
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Override
    public String getLock(String itemId) throws KeeperException, InterruptedException {
        Stat exists = zookeeper.exists(PARENT_PATH, false);
        if (exists == null) {
            // 创建分布式锁的“根” 持久化跟节点
            zookeeper.create(PARENT_PATH, "This is distribute lock parent path.".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        // 创建临时节点 /dlocks/itemId
        String lockPath = PARENT_PATH + "/" + itemId;

        int tryCount = 20; // 尝试20次
        int tryTime = 0;    // 已尝试的次数
        int tryDelay = 2000; //尝试间隔事件

        try {
            // 创建临时节点，随会话存在
            zookeeper.create(lockPath, "child node.".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            System.out.println("ok,I get lock" + Thread.currentThread());
        } catch (Exception e) {
            while (tryTime < tryCount) {
                Thread.sleep(tryDelay);
                tryTime++;
                try {
                    System.out.println("I tryTime= " + tryTime + " " + Thread.currentThread());
                    zookeeper.create(lockPath, "child node.".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                    System.out.println("I tryed= " + tryTime + " " + Thread.currentThread());
                } catch (Exception e1) {
                    continue;
                }
                break;
            }
        }
        return lockPath;
    }

    @Override
    public void releaseLock(String itemId) throws KeeperException, InterruptedException {
        zookeeper.delete(PARENT_PATH + "/" + itemId, -1);
    }


    /**
     * 可以 开启多个客户端（进程），使用该分布式锁
     *
     * @param args
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void main(String[] args) {
        CompetitiveDistributedLock distributedLock = new CompetitiveDistributedLock();

        String lock1 = "lock1";
        try {
            distributedLock.getLock(lock1);
            //deal data
            Thread.sleep(10000);

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            try {
                distributedLock.releaseLock(lock1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


}
