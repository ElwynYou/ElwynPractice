package designMode.singleton;

/**
 * @Package designMode.singleton
 * @Description: //todo(用一句话描述该文件做什么)
 * @Author elwyn
 * @Date 2017/6/18 13:41
 * @Email elonyong@163.com
 */
public class TestSing {
    public static void main(String[] args) {
        Resource instance = SingletonEnum.INSTANCE.getInstance();
        Resource instance1 = SingletonEnum.INSTANCE.getInstance();
        System.out.println(instance);
        System.out.println(instance1);
    }
}
