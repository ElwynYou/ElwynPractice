package designMode.factory;

import java.util.ArrayList;
import java.util.List;

/**
 * @Package designMode.factory
 * @Description:
 * @Author elwyn
 * @Date 2017/12/5 22:19
 * @Email elonyong@163.com
 */
public class IDCardFactory extends Factory {
    private List owners = new ArrayList();

    @Override
    protected void regosterProduct(Product product) {
        owners.add(((IDCard) product).getOwner());
    }

    @Override
    protected Product createProduct(String owner) {
        return new IDCard(owner);
    }

    public List getOwners() {
        return this.owners;
    }
}
