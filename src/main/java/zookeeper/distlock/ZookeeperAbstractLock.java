package zookeeper.distlock;


import org.I0Itec.zkclient.ZkClient;
import org.apache.log4j.Logger;

/**
 * @Package zookeeper.distlock
 * @Description:
 * @Author elwyn
 * @Date 2017/9/4 1:28
 * @Email elonyong@163.com
 */
public abstract class ZookeeperAbstractLock implements Lock {
    protected static String keeperAdress = "192.168.128.141:2181";
    protected static String path = "/lock";
    protected ZkClient zkClient = new ZkClient(keeperAdress);

    private static Logger logger = Logger.getLogger(ZookeeperAbstractLock.class);

    //阻塞试的获取锁
    @Override
    public void getLock() {
        if (tryLock()) {
            logger.info(Thread.currentThread().getName() + "get lock !");
        } else {
            waitForLock();
            getLock();
        }
    }


    @Override
    public void unLock() {
        zkClient.close();
    }

    protected abstract void waitForLock();

    protected abstract boolean tryLock();
}
