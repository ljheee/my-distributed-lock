package com.ljheee.lock;

import org.apache.zookeeper.KeeperException;

/**
 * Created by lijianhua04 on 2018/10/27.
 */
public interface IDistributedLock {
    public String getLock(String itemId) throws KeeperException, InterruptedException;

    public void releaseLock(String itemId) throws KeeperException, InterruptedException;
}
