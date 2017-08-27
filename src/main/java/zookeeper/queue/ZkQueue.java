package zookeeper.queue;

import org.apache.zookeeper.*;

import java.io.IOException;

/**
 * @Package zookeeper.queue
 * @Description:
 * @Author elwyn
 * @Date 2017/8/28 3:31
 * @Email elonyong@163.com
 */
public class ZkQueue {

    public static void main(String[] args) {

    }

    public static void doOne() throws IOException, KeeperException, InterruptedException {
        String host = "";
        ZooKeeper zooKeeper = connection(host);

        initQueue(zooKeeper);
        joinQueue(zooKeeper, 1);
        joinQueue(zooKeeper, 2);
        joinQueue(zooKeeper, 3);
        zooKeeper.close();
    }


    public static ZooKeeper connection(String host) throws IOException {
        ZooKeeper zooKeeper = new ZooKeeper(host, 6000, (e) -> {
            if (e.getType() == Watcher.Event.EventType.NodeCreated && e.getPath().equals("/queue")) {
                System.out.println("创建成功");
            }
            System.out.println("触发了------" + e.getType());
        });
        return zooKeeper;
    }

    public static void initQueue(ZooKeeper zooKeeper) throws KeeperException, InterruptedException {
        System.out.println("队列开始");
        if (zooKeeper.exists("/queue", false) == null) {
            System.out.println("创建队列");
            zooKeeper.create("/queue", "task-queue".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

        } else {
            System.out.println("队列已经存在");
        }
    }

    public static void joinQueue(ZooKeeper zooKeeper, int x) throws KeeperException, InterruptedException {
        zooKeeper.create("/queue/x" + x, ("x" + x).getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println("队列x" + x + "已经被创建");
    }

    public static void isCompleted(ZooKeeper zooKeeper) throws KeeperException, InterruptedException {
        int size = 3;//队列长度
        int length = zooKeeper.getChildren("/queue", true).size();
        System.out.println("创建队列完成,当前队列大小为:" + length + "/" + size);
        if (length >= size) {
            System.out.println("可以发车");
            zooKeeper.create("/queue/start", "start".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }
}
