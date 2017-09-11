package designMode.decorator.coffee;

public class HouseBlend extends Beverage {
    public HouseBlend(){
        description="House Blend coffee";
    }
    @Override
    public double cost() {
        return 4.9;
    }
}