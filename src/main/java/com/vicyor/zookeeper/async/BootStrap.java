package com.vicyor.zookeeper.async;

import org.apache.zookeeper.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * 作者:姚克威
 * 时间:2020/2/4 20:51
 **/
public class BootStrap implements Watcher {
    private Logger log = LoggerFactory.getLogger(BootStrap.class);
    ZooKeeper zooKeeper=null;
    public BootStrap()  {
    }

    public static void main(String[] args) {
        try {
            BootStrap  bootStrap=new BootStrap();
            bootStrap.startZk();
            bootStrap.bootstrap();
            System.in.read();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    void  startZk() throws IOException {
        zooKeeper = new ZooKeeper("192.168.78.129:2181", 1000, this);

    }

    @Override
    public void process(WatchedEvent event) {
        System.out.println(event);
    }

    AsyncCallback.StringCallback createParentCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    createParent(path, (byte[]) ctx);
                    break;
                case OK:
                    log.info("Parent created");
                    break;
                case NODEEXISTS:
                    log.error("Parent already registered: " + path);
            }
        }
    };

    /**
     * 设置元数据
     */
    public void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);
    }

    void createParent(String path, byte[] data) {
        zooKeeper.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, createParentCallback, data);
    }
}
