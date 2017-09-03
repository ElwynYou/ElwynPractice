package zookeeper.distlock;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.exception.ZkException;

import java.util.concurrent.CountDownLatch;

/**
 * @Package zookeeper.distlock
 * @Description:
 * @Author elwyn
 * @Date 2017/9/4 1:42
 * @Email elonyong@163.com
 */
public class ZookeeperDistributeLock extends ZookeeperAbstractLock {
    private CountDownLatch countDownLatch;

    public ZookeeperDistributeLock() {
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
                if (countDownLatch != null) {
                    countDownLatch.countDown();
                }
            }
        };
        zkClient.subscribeDataChanges(path, listener);
        if (zkClient.exists(path)) {
            countDownLatch = new CountDownLatch(1);
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        zkClient.unsubscribeDataChanges(path, listener);
    }

    @Override
    protected boolean tryLock() {
        try {
            if (!zkClient.exists(path)) {
                zkClient.createEphemeral(path);
                return true;
            } else return false;
        } catch (ZkException e) {
            e.printStackTrace();
            return false;
        }
    }
}
