package designMode.cor2;

/**
 * @Package designMode.cor2
 * @Description:
 * @Author elwyn
 * @Date 2017/12/7 23:49
 * @Email elonyong@163.com
 */
public class Trouble {
    private int number;

    public Trouble(int number) {
        this.number = number;
    }

    public int getNumber() {
        return number;
    }

    @Override
    public String toString() {
        return "Trouble{" +
                "number=" + number +
                '}';
    }
}
