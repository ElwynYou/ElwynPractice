package designMode.cor2;

/**
 * @Package designMode.cor2
 * @Description:
 * @Author elwyn
 * @Date 2017/12/8 0:03
 * @Email elonyong@163.com
 */
public class Main {
    public static void main(String[] args) {
        Support alice = new NoSupport("Alice");
        Support bob = new LimitSupport("bob",100);
        Support charlie = new SpecialSupport("charlie",429);
        Support diana = new LimitSupport("diana",200);
        Support elmo = new OddSupport("elmo");
        Support fred = new LimitSupport("Fred",300);
        alice.setNext(bob).setNext(charlie).setNext(diana).setNext(elmo).setNext(fred);
        for (int i = 0; i < 500; i+=33) {
            alice.support(new Trouble(i));
        }

    }
}
