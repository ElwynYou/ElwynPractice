package juc;

/**
 * @Package juc
 * @Description:
 * @Author elwyn
 * @Date 2017/9/15 1:36
 * @Email elonyong@163.com
 */
public class TestVolatile {
    public static void main(String[] args) {
        ThreadDemo1 threadDemo1 = new ThreadDemo1();
        new Thread(threadDemo1).start();
        while (true) {
            if (threadDemo1.isflag()) {
                System.out.println("________________________");
                break;
            }
        }
    }
}

class ThreadDemo1 implements Runnable {
    private boolean flag = false;

    @Override
    public void run() {
        try {
            Thread.sleep(200);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        flag = true;
        System.out.println("flag=" + isflag());
    }

    public boolean isflag() {
        return flag;
    }
}