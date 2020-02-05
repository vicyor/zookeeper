package com.vicyor.zookeeper.async;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
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
        log.error("启动ZK");
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    public void process(WatchedEvent e) {
        log.error("zk会话事件=>"+e.toString());
    }

    public void stopZK() throws Exception {
        zk.close();
    }


    private Random random = new Random();

    private String serverId = Integer.toHexString(random.nextInt());

    private boolean isLeader = false;

    public void runForMaster() {
        log.error("竞争主节点");
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
                    log.error("成为主节点");
                    getWorkers();
                    getTasks();
                    break;
                case NODEEXISTS:
                    log.error("已经存在主节点");
                    isLeader = false;
                    masterExists();
                    break;
            }

        }
    };

    Watcher masterExistsWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeDeleted) {
                log.error("系统的已有主节点宕机");
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

    private List<String> workers = new ArrayList<>();
    Watcher workersChangeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                log.error("/workers 子节点改变");
                assert "/workers".equals(event.getPath());
                getWorkers();
            }
        }
    };
    AsyncCallback.ChildrenCallback workersGetChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    log.info("成功的获取workers列表");
                    reassignAndSet(children);
            }
        }
    };

    void reassignAndSet(List<String> children) {
        List<String> expired = new ArrayList<>();
        for (String worker : children) {
            if (!workers.contains(worker)) expired.add(worker);
        }
        workers = children;
        /**
         * 对过期的worker的任务的处理
         */
        for (String eWorker : expired) {

            getAbsentWorkerTasks(eWorker);
        }
    }

    void getAbsentWorkerTasks(String eWorker) {
        log.error("重新分配宕机eWorker=>未处理的任务");
        try {
            List<String> children = zk.getChildren("/assign/" + eWorker, false);
            for (String task : children) {
                Stat stat = new Stat();
                byte[] data = null;
                try {
                    data = zk.getData("/assign/" + eWorker + "/" + task + "/done", false, stat);
                } catch (KeeperException e) {
                    //未完成的任务重做
                    if (e.code() == KeeperException.Code.NONODE) {
                        int workerIndex = random.nextInt(workers.size());
                        String worker = workers.get(workerIndex);
                        data = zk.getData("/assign/" + eWorker + "/" + task, false, stat);
                        zk.create("/assign/" + worker + "/" + task, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, assignTaskCallback, null);
                        deleteAssignTask(eWorker,task);
                    }
                }
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    void deleteAssignTask(String eWork,String task) {
        try {
            zk.delete("/assign/"+eWork+"/"+task,-1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    void getWorkers() {
        log.error("获取workers");
        zk.getChildren("/workers", workersChangeWatcher, workersGetChildrenCallback, null);
    }

    Watcher tasksChangeWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent event) {
            if (event.getType() == Event.EventType.NodeChildrenChanged) {
                log.error("/tasks 子节点改变");
                assert "/tasks".equals(event.getPath());
                getTasks();
            }
        }
    };
    private List<String> tasks;
    AsyncCallback.ChildrenCallback tasksGetChildrenCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    if (children != null) {
                        log.error("成功的获取任务");
                        tasks = children;
                        assignTasks(children);
                    }
            }
        }
    };

    void getTasks() {
        log.error("开始获取任务");
        zk.getChildren("/tasks", tasksChangeWatcher, tasksGetChildrenCallback, null);
    }

    void assignTasks(List<String> tasks) {
        log.error("进行任务分配");
        for (String task : tasks) {
            getTaskData(task);
        }
    }

    AsyncCallback.DataCallback taskDataCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    /**
                     * 随机的选择一个worker,处理
                     */
                    int workerIndex = random.nextInt(workers.size());
                    String worker = workers.get(workerIndex);
                    log.error("选择一个worker:{%s }处理task:{%s}",worker,ctx);
                    String assignmentPath = "/assign/" + worker + "/" + ctx;
                    createAssignment(assignmentPath, data);
                    //有可能任务被其它worker处理删除了
                case NODEEXISTS:
                    break;
            }
        }
    };

    void getTaskData(String task) {
        zk.getData("/tasks/" + task, false, taskDataCallback, task);
    }

    AsyncCallback.StringCallback assignTaskCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    log.info("任务分配完成");
                    deleteTask(name.substring(name.lastIndexOf("/") + 1));
            }
        }
    };

    void deleteTask(String task) {
        try {
            log.error("删除/tasks/{%s}",task);
            zk.delete("/tasks/" + task, -1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }

    void createAssignment(String path, byte[] data) {
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, assignTaskCallback, null);
    }

    public static void main(String[] args) throws Exception {
        Master m = new Master();
        m.startZK();
        m.runForMaster();
        System.in.read();
        m.stopZK();
    }


}
