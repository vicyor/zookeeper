package com.vicyor.zookeeper.async;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 作者:姚克威
 * 时间:2020/2/4 13:10
 **/
public class Master implements Watcher {
    private ZooKeeper zk;
    private String hostPort = "192.168.78.129:2181";
    private Logger log = LoggerFactory.getLogger(Master.class);

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    public void process(WatchedEvent e) {
        log.info(e.toString());
    }

    public void stopZK() throws Exception {
        zk.close();
    }


    private Random random = new Random();

    private String serverId = Integer.toHexString(random.nextInt());

    private boolean isLeader = false;

    public void runForMaster() {
        zk.create("/master", serverId.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL, masterCreateCallback, null);
    }

    /**
     * 创建/master节点 回调
     */
    AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    isLeader = true;
                    log.info("I'm the leader");
                    break;
                case NODEEXISTS:
                    log.info("已经存在主节点");
                    isLeader = false;
                    masterExists();
                    break;
            }

        }
    };

    Watcher masterExistsWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            System.err.println(event);
            if (event.getType() == Event.EventType.NodeDeleted) {
                log.info("主节点宕机");
                runForMaster();
            }
        }
    };

    void masterExists() {
        try {
            zk.exists("/master", masterExistsWatcher);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    private List<String>workers=new ArrayList<>();
    Watcher workersChangeWatcher=new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if(event.getType()== Event.EventType.NodeChildrenChanged){
                assert  "/workers".equals(event.getPath());
                getWorkers();
            }
        }
    };
    AsyncCallback.ChildrenCallback workersGetChildrenCallback=new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (KeeperException.Code.get(rc)){
                case OK:
                    log.info("成功的获取workers列表");
                    reassignAndSet(children);
            }
        }
    };
    void reassignAndSet(List<String>children){
        List <String>expired=new ArrayList<>();
        for(String worker:children){
            if(!workers.contains(worker)) expired.add(worker);
        }
        workers=children;
        /**
         * 对过期的worker处理
         */
         for(String e:expired){
             //TODO
             //getAbsentWorkerTasks(worker);
         }
    }
    void getWorkers(){
        zk.getChildren("/workers",workersChangeWatcher,workersGetChildrenCallback,null);
    }
    public static void main(String[] args) throws Exception {
        Master m = new Master();
        m.startZK();
        m.runForMaster();
        Thread.sleep(1000000000);
        m.stopZK();
    }


}
