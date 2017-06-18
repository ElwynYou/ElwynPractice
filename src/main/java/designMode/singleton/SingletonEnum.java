package designMode.singleton;

/**
 * @Package designMode.singleton
 * @Description: //todo(用一句话描述该文件做什么)
 * @Author elwyn
 * @Date 2017/6/18 13:28
 * @Email elonyong@163.com
 */
 class Resource {

}

public enum SingletonEnum {
    INSTANCE;
    private final  Resource  resource;

    SingletonEnum() {
        resource = new Resource();
    }

    public Resource getInstance() {
        return resource;
    }
}
