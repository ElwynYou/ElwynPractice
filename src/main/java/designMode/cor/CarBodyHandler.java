package designMode.cor;

/**
 * @Package designMode.cor
 * @Description:
 * @Author elwyn
 * @Date 2017/12/7 23:12
 * @Email elonyong@163.com
 */
public class CarBodyHandler extends CarHandler {

    @Override
    void handerCar() {
        System.out.println("组装车身");
        if (this.carHandler!=null){
            this.carHandler.handerCar();
        }
    }
}
