package designMode.factory;

/**
 * @Package designMode.factory
 * @Description:
 * @Author elwyn
 * @Date 2017/12/5 22:16
 * @Email elonyong@163.com
 */
public class IDCard extends Product {
    private String owner;

    IDCard(String owner) {
        System.out.println("制作" + owner + "的ID卡");
        this.owner = owner;

    }

    @Override
    void use() {
        System.out.println("使用" + owner + "的ID卡");
    }

    public String getOwner() {
        return this.owner;
    }
}
