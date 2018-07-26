package com.ljheee.zk.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 懒汉式：基于时间通知
 */
public class DistributedLock {

    private ZooKeeper zookeeper;
    private CountDownLatch countDownLatch = new CountDownLatch(1);


    // 当前会话关联的最后一级znode，关联后，在当前会话过程中，将能够访问该节点以及其子节点，而不能访问该节点的上级节点和旁系节点，该节点可以视作是当前会话的根节点。
    private String connectString = "192.168.1.200:2181/myTest";


    private static String PARENT_PATH = "dlocks";

    public DistributedLock() {

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


    public String getLock(String itemId) throws KeeperException, InterruptedException {
        Stat exists = zookeeper.exists(PARENT_PATH, false);
        if (exists == null) {
            // 创建分布式锁 持久化跟节点
            zookeeper.create(PARENT_PATH, "This is distribute lock parent path.".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        //
        String id_path = PARENT_PATH + "/" + itemId;// 存放ID 的父节点
        Stat exists1 = zookeeper.exists(id_path, false);

        if (exists1 == null) {
            zookeeper.create(id_path, "This is distribute lock ids node".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }


        // 创建 唯一标识的子节点
        String lock_path = id_path + "/lock_";

        String current_path = zookeeper.create(id_path, "This is distribute lock".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);


        List<String> children = zookeeper.getChildren(current_path, false);

        if (children.size() == 1) {
            // ok
            return id_path;// todo children.get(0)
        }

        // 开启监听，监听带序号的  上一个
        String current_path_id = current_path.substring(current_path.indexOf("lock_") + 5);
        int pre_id = Integer.parseInt(current_path_id) - 1;


        String pre_path = id_path + "/lock_" + String.format("%010d", pre_id);
        zookeeper.exists(pre_path, new Watcher() {
            public void process(WatchedEvent watchedEvent) {

                if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
                    // ok
                    countDownLatch.countDown();
                }
            }
        });

        countDownLatch.await();
        return current_path;

    }

    public void releaseLock(String itemId) throws KeeperException, InterruptedException {
        zookeeper.delete(itemId, -1);
    }


    /**
     * 可以 开启多个客户端（进程），使用该分布式锁
     *
     * @param args
     * @throws KeeperException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws KeeperException, InterruptedException {
        DistributedLock distributedLock = new DistributedLock();

        String lock = distributedLock.getLock("want");

        //deal data
        Thread.sleep(10000);
        distributedLock.releaseLock(lock);

    }


}
