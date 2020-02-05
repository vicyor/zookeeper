package com.vicyor.zookeeper.async;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * 作者:姚克威
 * 时间:2020/2/4 14:32
 **/
public class Worker implements Watcher {

    private static final Logger log = LoggerFactory.getLogger(Worker.class);
    private ZooKeeper zk;
    private String hostPort = "192.168.78.129:2181";
    private Random random = new Random();
    private String serverId = Integer.toHexString(random.nextInt());
    private String name = "worker-" + serverId;
    private CountDownLatch latch = new CountDownLatch(1);
    private volatile String status;

    Worker(String hostPort) {
        this.hostPort = hostPort;
    }

    Worker() {

    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    @Override
    public void process(WatchedEvent event) {
        log.info(event.toString() + "," + hostPort);
    }

    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case OK:
                    log.info("Registered successfully: " + serverId);
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

    /**
     * 异步更新结果处理
     */
    AsyncCallback.StatCallback statUpCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    /**
                     * 保证安全性,异步status可能更新很慢,该worker其它线程可能又更新了status
                     */
                    if (ctx.toString().equals(status))
                        updateStatus((String) ctx);
            }
        }
    };

    /**
     * 更新worker状态
     * @param status
     */
    void updateStatus(String status) {
        this.status = status;
        //context <=> status
        zk.setData("/workers/" + name, status.getBytes(), -1, statUpCallback, status);
    }


    public static void main(String[] args) throws IOException, InterruptedException {
        Worker w = new Worker();
        w.startZK();
        w.register();
        w.latch.await();
    }
}
