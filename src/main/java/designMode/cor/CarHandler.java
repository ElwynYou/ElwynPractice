package designMode.cor;

/**
 * @Package designMode.cor
 * @Description:
 * @Author elwyn
 * @Date 2017/12/7 23:10
 * @Email elonyong@163.com
 */
public abstract class CarHandler {
    protected CarHandler carHandler;

    public CarHandler setNextHandler(CarHandler carHandler) {
        this.carHandler = carHandler;
        return this.carHandler;
    }

    abstract void handerCar();
}
