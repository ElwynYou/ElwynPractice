package zookeeper.distlock;

import org.I0Itec.zkclient.IZkDataListener;
import org.apache.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * @Package zookeeper.distlock
 * @Description:
 * @Author elwyn
 * @Date 2017/9/4 2:32
 * @Email elonyong@163.com
 */
public class ZookeeperImproveLock extends ZookeeperAbstractLock {
    private static Logger logger = Logger.getLogger(ZookeeperAbstractLock.class);
    private String beforePath;
    private String currentPath;
    private CountDownLatch countDownLatch;

    public ZookeeperImproveLock() {
        if (!this.zkClient.exists(path)) {
            this.zkClient.createPersistent(path, "lock");
        }
    }


    @Override
    public void getLock() {
        super.getLock();
    }

    @Override
    public void unLock() {
        super.unLock();
    }

    @Override
    protected void waitForLock() {
        IZkDataListener listener = new IZkDataListener() {
            @Override
            public void handleDataChange(String dataPath, Object data) throws Exception {

            }

            @Override
            public void handleDataDeleted(String dataPath) throws Exception {
                logger.info(Thread.currentThread().getName() + "捕获到DataDelete事件----------------------");
                if (countDownLatch != null) {
                    countDownLatch.countDown();
                }
            }
        };
        this.zkClient.subscribeDataChanges(beforePath, listener);
        if (this.zkClient.exists(beforePath)) {
            countDownLatch = new CountDownLatch(1);
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        this.zkClient.unsubscribeDataChanges(beforePath, listener);
    }

    @Override
    protected boolean tryLock() {
        if (currentPath == null || currentPath.length() <= 0) {
            currentPath = this.zkClient.createEphemeralSequential(path + "/", "lock");
        }
        List<String> children = this.zkClient.getChildren(path);
        Collections.sort(children);
        if (currentPath.equals(path + "/" + children.get(0))) {
            return true;
        } else {
            int wz = Collections.binarySearch(children, currentPath.substring(6));
            beforePath = path + "/" + children.get(wz - 1);
        }
        return false;
    }
}
