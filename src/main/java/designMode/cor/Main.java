package designMode.cor;

/**
 * @Package designMode.cor
 * @Description:
 * @Author elwyn
 * @Date 2017/12/7 23:28
 * @Email elonyong@163.com
 */
public class Main {
    public static void main(String[] args) {
        CarHandler carHandler1=new CarHeadHandler();
        CarHandler carHandler2=new CarBodyHandler();
        CarHandler carHandler3=new CarTailHandler();
      /*  carHandler1.setNextHandler(carHandler2);
        carHandler2.setNextHandler(carHandler3);
        carHandler1.handerCar();*/


        carHandler1.setNextHandler(carHandler2).setNextHandler(carHandler3);
        carHandler1.handerCar();
    }
}
