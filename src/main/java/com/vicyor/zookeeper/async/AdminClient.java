package com.vicyor.zookeeper.async;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.util.Date;
import java.util.concurrent.CountDownLatch;

/**
 * 作者:姚克威
 * 时间:2020/2/4 15:32
 **/
public class AdminClient implements Watcher {
    private ZooKeeper zk;
    private String hostPort = "192.168.78.129:2181";
    private CountDownLatch latch = new CountDownLatch(1);

    AdminClient(String hostPort) {
        this.hostPort = hostPort;
    }

    AdminClient() {
    }

    void start() throws Exception {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println(event);
    }

    public void listState() throws InterruptedException, KeeperException {
        Stat stat = new Stat();
        byte[] masterData = new byte[0];
        try {
            masterData = zk.getData("/master", false, stat);
            Date startDate = new Date(stat.getCtime());
            System.out.println("Master: " + new String(masterData) + " since " + startDate);
        } catch (KeeperException e) {
            System.out.println("No Master");
        }
        System.out.println("Workers:");
        for (String w : zk.getChildren("/workers", false)) {
            byte data[] = zk.getData("/workers/" + w, false, null);
            String state = new String(data);
            System.out.println("\t" + w + " : " + state);
        }
        System.out.println("Tasks");
        for (String t : zk.getChildren("/tasks", false)) {
            System.out.println("\t" + t);
        }
        System.out.println("Assigns");
        for (String t : zk.getChildren("/assign", false)) {
            System.out.println("\t" + t);
        }
    }

    public static void main(String[] args) throws Exception {
        AdminClient adminClient = new AdminClient();
        adminClient.start();
        adminClient.listState();
        adminClient.latch.await();
    }
}
