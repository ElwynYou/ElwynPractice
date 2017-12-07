package designMode.factory;

/**
 * @Package designMode.factory
 * @Description:
 * @Author elwyn
 * @Date 2017/12/5 22:22
 * @Email elonyong@163.com
 */
public class Main {
    public static void main(String[] args) {
        Factory factory=new IDCardFactory();
        Product product1 = factory.create("小明");
        Product product2 = factory.create("小红");
        Product product3 = factory.create("小刚");
        product1.use();
        product2.use();
        product3.use();
    }
}
