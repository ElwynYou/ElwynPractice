package zookeeper.lock;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @Package zookeeper.lock
 * @Description:
 * @Author elwyn
 * @Date 2017/8/28 2:42
 * @Email elonyong@163.com
 */
public class ZkLock implements Watcher {
    private ZooKeeper zooKeeper;
    String root = "/root";
    private String path;
    private String currentNode;
    private String waitNode;
    CountDownLatch countDownLatch;

    public ZkLock(String host, String path) {
        this.path = path;
        try {
            zooKeeper = new ZooKeeper(host, 5000, this);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            Stat stat = zooKeeper.exists(root, false);
            if (null == stat) {
                //root节点必须是永久的
                zooKeeper.create(root, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }


    public void lock() throws KeeperException, InterruptedException {
        try {
            //创建临时有序的节点
            currentNode = zooKeeper.create(root + "/" + path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
        //获取所有子节点
        List<String> childrenList = zooKeeper.getChildren(root, false);
        //排序的目的是为了保证拿到第一个节点是最小的节点
        Collections.sort(childrenList);
        if (currentNode.equals(root + "/" + childrenList.get(0))) {
            //如果当前节点就是最小节点,直接返回
            return;
        } else {
            String childZnode = currentNode.substring(currentNode.lastIndexOf("/") + 1);
            int num = Collections.binarySearch(childrenList, childZnode);
            //获取上一个节点
            waitNode = childrenList.get(num - 1);
            //判断节点是否存在,并且监听
            Stat stat = zooKeeper.exists(root + "/" + waitNode, true);
            if (stat != null) {
                countDownLatch = new CountDownLatch(1);
                this.countDownLatch.await(5000, TimeUnit.MILLISECONDS);
            }

        }
    }


    public void unLock() {
        try {
            zooKeeper.delete(currentNode, -1);
            currentNode = null;
            zooKeeper.close();
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }


    @Override
    public void process(WatchedEvent watchedEvent) {
        if (countDownLatch != null) {
            countDownLatch.countDown();
        }
    }


}
