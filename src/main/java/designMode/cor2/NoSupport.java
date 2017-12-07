package designMode.cor2;

/**
 * @Package designMode.cor2
 * @Description:
 * @Author elwyn
 * @Date 2017/12/7 23:57
 * @Email elonyong@163.com
 */
public class NoSupport extends Support {
    public NoSupport(String name) {
        super(name);
    }

    @Override
    protected boolean resolve(Trouble trouble) {
        return false;
    }
}
