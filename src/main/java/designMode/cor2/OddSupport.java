package designMode.cor2;

/**
 * @Package designMode.cor2
 * @Description:
 * @Author elwyn
 * @Date 2017/12/7 23:59
 * @Email elonyong@163.com
 */
public class OddSupport extends Support {
    public OddSupport(String name) {
        super(name);
    }

    @Override
    protected boolean resolve(Trouble trouble) {
        if (trouble.getNumber() % 2 == 1) {
            return true;
        }
        return false;
    }
}
