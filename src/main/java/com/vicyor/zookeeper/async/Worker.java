package com.vicyor.zookeeper.async;

import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 作者:姚克威
 * 时间:2020/2/4 14:32
 **/
@Slf4j
public class Worker implements Watcher {

    private ZooKeeper zk;
    private String hostPort = "192.168.78.129:2181";
    private Random random = new Random();
    private String serverId = Integer.toHexString(random.nextInt());
    private String name = "worker-" + serverId;
    private CountDownLatch latch = new CountDownLatch(1);

    Worker(String hostPort) {
        this.hostPort = hostPort;
    }

    Worker() {

    }

    void startZK() throws IOException {
        log.error("启动ZooKeeper");
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    @Override
    public void process(WatchedEvent event) {
        log.error(event.toString() + "," + hostPort);
    }

    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    log.error("成功注册worker: " + serverId);
                    createAssign();
                    getTasks();
                    break;
                case NODEEXISTS:
                    log.warn("Already registered: " + serverId);
                    break;
                default:
                    log.error("Something went wrong: " + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    /**
     * 向Zookeeper注册
     */
    void register() {
        zk.create("/workers/worker-" + serverId,
                "Idle".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                createWorkerCallback, null
        );

    }

    //创建assign
    void createAssign() {
        try {
            log.error("创建/assign/{%s}",name);
            zk.create("/assign/" + name, name.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }



    Watcher newTaskWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                assert ("/assign/" + name).equals(event.getPath());
                getTasks();
            }

        }
    };

    public void getTasks() {
        log.error("获取任务列表");
        zk.getChildren("/assign/" + name, newTaskWatcher, tasksGetChildrenCallback, null);
    }

    ExecutorService threadPool = Executors.newCachedThreadPool();
    AsyncCallback.ChildrenCallback tasksGetChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    if (children != null) {
                        children.forEach(taskPath -> {
                            Stat stat = new Stat();
                            byte[] data = null;
                            try {
                                data = zk.getData("/assign/" + name + "/" + taskPath + "/done", false, stat);
                            } catch (KeeperException e) {
                                if (e.code() == KeeperException.Code.NONODE) {
                                    try {
                                        data = zk.getData("/assign/" + name + "/" + taskPath, false, stat);
                                    } catch (KeeperException ex) {
                                        ex.printStackTrace();
                                    } catch (InterruptedException ex) {
                                        ex.printStackTrace();
                                    }
                                    Task task = new Task(data, "/assign/" + name + "/" + taskPath);
                                    threadPool.execute(() -> {
                                        log.info(name + "开始执行任务");
                                        task.execute();
                                        log.info("任务执行结束");
                                        task.afterExecuted(zk);
                                    });
                                }
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                        });
                    }
            }
        }
    };

    public static void main(String[] args) throws IOException, InterruptedException {
        Worker w = new Worker();
        w.startZK();
        w.register();
        w.latch.await();
    }
}
