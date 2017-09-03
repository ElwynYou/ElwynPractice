package zookeeper.distlock;

/**
 * @Package zookeeper.distlock
 * @Description:
 * @Author elwyn
 * @Date 2017/9/4 1:27
 * @Email elonyong@163.com
 */
public interface Lock {
    void getLock();

    void unLock();
}
