package com.vicyor.zookeeper.async;

import org.apache.zookeeper.*;

import java.util.concurrent.CountDownLatch;

/**
 * 作者:姚克威
 * 时间:2020/2/4 15:19
 **/
public class Client implements Watcher {
    @Override
    public void process(WatchedEvent event) {
        System.out.println(event);
    }
    private CountDownLatch latch=new CountDownLatch(1);
    private ZooKeeper zk;
    private String hostPort="192.168.78.129:2181";

    Client(String hostPort) {
        this.hostPort = hostPort;
    }
   Client(){

   }
    void startZK() throws Exception {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    String queueCommand(String command) throws KeeperException {
        while (true) {
            try {
                String name = zk.create("/tasks/task-", command.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                return name;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Client c=new Client();
        c.startZK();
        String name= c.queueCommand("hello,world");
        System.out.println("Created "+name);
        c.latch.await();
    }
}
