package designMode.adapter;

/**
 * @Package designMode.adapter
 * @Description:
 * @Author elwyn
 * @Date 2017/12/4 22:16
 * @Email elonyong@163.com
 */
public class Banner {
    private String string;

    public Banner(String string) {
        this.string = string;
    }

    public void showWithParen() {
        System.out.println("(" + string + ")");
    }

    public void showWithAster() {
        System.out.println("*" + string + "*");
    }
}
