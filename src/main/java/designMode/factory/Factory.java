package designMode.factory;

/**
 * @Package designMode.factory
 * @Description:
 * @Author elwyn
 * @Date 2017/12/5 22:12
 * @Email elonyong@163.com
 */
public abstract class Factory {
    public final Product create(String owner){
        Product product = createProduct(owner);
        regosterProduct(product);
        return product;
    }

    protected abstract void regosterProduct(Product product);

    protected abstract Product createProduct(String owner);
}
