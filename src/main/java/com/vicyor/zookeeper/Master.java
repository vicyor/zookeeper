package com.vicyor.zookeeper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.sql.SQLOutput;
import java.util.Random;

import static org.apache.zookeeper.KeeperException.Code.NODEEXISTS;

/**
 * 作者:姚克威
 * 时间:2020/2/4 12:23
 **/
public class Master implements Watcher {
    ZooKeeper zk;
    String hostPort = "192.168.78.129:2181";

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

    public boolean checkMaster() {
        while (true) {
            try {
                Stat stat = new Stat();
                byte[] data = zk.getData("/master", false, stat);
                isLeader = new String(data).equals(serverId);
                return isLeader;
            } catch (KeeperException e) {
                return false;
            } catch (InterruptedException e) {
            }
        }
    }

    public void runForMaster() {
        /**
         * 自旋
         */
        while (true) {
            try {
                zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                isLeader = true;
                break;
            } catch (KeeperException e) {
                switch (e.code()) {
                    case NODEEXISTS:
                        isLeader = false;
                        break;
                    case CONNECTIONLOSS:
                        checkMaster();
                }
                isLeader = false;
                break;
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            if (isLeader) {
                break;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Master m = new Master();
        m.startZK();
        Thread.sleep(6000);
        m.runForMaster();
        if(m.isLeader){
            System.out.println("master:"+m.serverId);
        }
        m.stopZK();
    }
}