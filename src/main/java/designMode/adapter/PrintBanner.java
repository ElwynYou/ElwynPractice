package designMode.adapter;

/**
 * @Package designMode.adapter
 * @Description:
 * @Author elwyn
 * @Date 2017/12/4 22:21
 * @Email elonyong@163.com
 */
public class PrintBanner extends Print {
    private Banner banner;
    public PrintBanner(String string) {
        this.banner = new Banner(string);
    }

    public void printWeak() {
        banner.showWithParen();
    }

    public void printStrong() {
        banner.showWithAster();
    }
}
