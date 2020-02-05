package com.vicyor.zookeeper.async;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

/**
 * 作者:姚克威
 * 时间:2020/2/5 14:03
 **/
public class Task {
    private  byte[] data;
    private String path;
    public Task(byte[] data, String path){
        this.data=data;
        this.path=path;
    }
    public void execute(){
        System.out.println(new String(data));
    }

    public void afterExecuted(ZooKeeper zk) {
        try {
            zk.create(path+"/done","".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
