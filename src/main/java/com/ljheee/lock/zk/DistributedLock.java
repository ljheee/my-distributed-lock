package com.ljheee.lock.zk;

import com.ljheee.lock.IDistributedLock;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * 懒汉式：基于事件通知
 */
public class DistributedLock implements IDistributedLock {

    private ZooKeeper zookeeper;
    private CountDownLatch countDownLatch = new CountDownLatch(1);
    private CountDownLatch countDownLatch2 = new CountDownLatch(1);


    // 当前会话关联的最后一级znode，关联后，在当前会话过程中，将能够访问该节点以及其子节点，而不能访问该节点的上级节点和旁系节点，该节点可以视作是当前会话的根节点。
    private String connectString = "127.0.0.1:2181/myTest";

    // 根节点
    private static String PARENT_PATH = "/dlocks";

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


    @Override
    public String getLock(String itemId) throws KeeperException, InterruptedException {

        // 检查 跟节点
        Stat exists = zookeeper.exists(PARENT_PATH, false);
        if (exists == null) {
            // 创建分布式锁 持久化 根节点
            zookeeper.create(PARENT_PATH, "This is distribute lock root path.".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }

        //
        String id_path = PARENT_PATH + "/" + itemId;// 存放[有序临时]ID 的父节点
        Stat exists1 = zookeeper.exists(id_path, false);

        if (exists1 == null) {
            zookeeper.create(id_path, "This is distribute lock ids node".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }


        // 创建 唯一标识的子节点
        String lock_path = id_path + "/lock_";//  want/lock_0000000001 want/lock_0000000002  序号从1开始

        String current_path = zookeeper.create(lock_path, "This is distribute lock".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("current_path=" + current_path);

        // 检查兄弟节点
        List<String> childrens = zookeeper.getChildren(id_path, false);

        if (childrens.size() == 1) {
            // ok 你是第一个来创建EPHEMERAL_SEQUENTIAL的，直接获得锁，序号路径即为current_path
            return current_path;
        }

        // 开启监听，监听带序号的 上一个节点（因为按客户端请求顺序，也是创建的带递增序号的临时节点，你什么时候能拿到锁，取决于你前面一个人什么时候结束）
        String current_path_id = current_path.substring(current_path.indexOf("lock_") + 5);// 截取获得当前节点序号
        int pre_id = Integer.parseInt(current_path_id) - 1;//上一个节点序号


        String pre_path = id_path + "/lock_" + String.format("%010d", pre_id);//上一个节点路径(总共10位，前面补0)
        System.out.println("pre_path=" + pre_path);

        // 监听上一个节点路径（结束事件）
        zookeeper.exists(pre_path, new Watcher() {
            public void process(WatchedEvent watchedEvent) {

                if (watchedEvent.getType() == Event.EventType.NodeDeleted) {
                    // 上一节点释放，当前进程获得锁
                    countDownLatch2.countDown();
                }
            }
        });

        countDownLatch2.await();
        return current_path;

    }

    /**
     * 删除临时节点
     * @param itemId
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Override
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
        System.out.println("I get lock=" + lock);

        //deal data
        Thread.sleep(10000);
        distributedLock.releaseLock(lock);

    }


}
