package designMode.cor2;

/**
 * @Package designMode.cor2
 * @Description:
 * @Author elwyn
 * @Date 2017/12/7 23:52
 * @Email elonyong@163.com
 */
public abstract class Support {
    private String name;
    private Support next;

    public Support(String name) {
        this.name = name;
    }

    public Support setNext(Support next) {
        this.next = next;
        return next;
    }

    public final void support(Trouble trouble) {
        if (resolve(trouble)) {
            done(trouble);
        } else if (next != null) {
            next.support(trouble);
        } else {
            fail(trouble);
        }
    }


    protected abstract boolean resolve(Trouble trouble);

    protected void done(Trouble trouble) {
        System.out.println(trouble + " is resolved by " + this.name);
    }

    protected void fail(Trouble trouble) {
        System.out.println(trouble + " cannot ben resolved ");
    }
}
