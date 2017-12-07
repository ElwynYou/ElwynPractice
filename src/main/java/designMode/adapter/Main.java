package designMode.adapter;

/**
 * @Package designMode.adapter
 * @Description:
 * @Author elwyn
 * @Date 2017/12/4 22:23
 * @Email elonyong@163.com
 */
public class Main {
    public static void main(String[] args) {
        Print print = new PrintBanner("Hello");
        print.printWeak();
        print.printStrong();
    }
}
