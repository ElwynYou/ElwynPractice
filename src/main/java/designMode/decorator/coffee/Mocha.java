package designMode.decorator.coffee;

public class Mocha extends CondimentDecorator {
    protected Beverage beverage;
    public Mocha(Beverage beverage){
        this.beverage=beverage;
    }
    @Override
    public String getDescription() {
        return beverage.getDescription()+",with Mocha";
    }
    @Override
    public double cost() {
        return 1.2+beverage.cost();
    }
}