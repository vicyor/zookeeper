package com.vicyor.zookeeper.async;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * 作者:姚克威
 * 时间:2020/2/4 13:10
 **/
public class Master implements Watcher {
    ZooKeeper zk;
    String hostPort = "192.168.78.129:2181";
    CountDownLatch latch = new CountDownLatch(1);

    Master(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    public void process(WatchedEvent e) {
        System.out.println(e);
    }

    public void stopZK() throws Exception {
        zk.close();
    }

    public Master() {
    }

    private Random random = new Random();

    private String serverId = Integer.toHexString(random.nextInt());
    private boolean isLeader = false;

    public void checkMaster() {
        zk.getData("/master", false, masterCheckCallback, null);
    }

    public void runForMaster() {
        zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, null);
    }

    AsyncCallback.DataCallback masterCheckCallback = new AsyncCallback.DataCallback() {

        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case NODEEXISTS:
                    runForMaster();
                    return;
            }
        }
    };
    /**
     * 创建/master节点 回调
     */
    AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    return;
                case OK:
                    isLeader = true;
                    break;
                case NODEEXISTS:
                    isLeader = false;
                    checkMaster();
                    return;
            }
            System.out.println("I'm the leader");
        }
    };
    AsyncCallback.StringCallback createParentCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    createParent(path, (byte[]) ctx);
                    break;
                case OK:
                    System.out.println("Parent created");
                    break;
                case NODEEXISTS:
                    System.out.println("Parent already registered: " + path);
            }
        }
    };

    void createParent(String path, byte[] data) {
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallback, data);
    }

    /**
     * 设置元数据
     */
    public void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    public static void main(String[] args) throws Exception {
        Master m = new Master();
        m.startZK();
        m.bootstrap();
        m.runForMaster();
        m.latch.await();
        m.stopZK();

    }
}
