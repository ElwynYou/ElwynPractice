package designMode.prototype;

import java.io.*;

/**
 * @Name designMode.prototype.Prototype
 * @Description
 * @Author Elwyn
 * @Version 2017/6/12
 * @Copyright 上海云辰信息科技有限公司
 **/
public class Prototype implements Cloneable,Serializable {

	private static final long serialVersionUID = 1L;
	private String string;
	private SerializableObject obj;
	@Override
	protected Object clone() throws CloneNotSupportedException {
		return super.clone();
	}

	public Object deepClone() throws IOException, ClassNotFoundException {
		 /* 写入当前对象的二进制流 */
		ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
		ObjectOutputStream objectOutputStream=new ObjectOutputStream(byteArrayOutputStream);
		objectOutputStream.writeObject(this);
      /* 读出二进制流产生的新对象 */
		ByteArrayInputStream byteArrayInputStream= new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
		ObjectInputStream objectInputStream=new ObjectInputStream(byteArrayInputStream);
		return objectInputStream.readObject();
	}
	public String getString() {
		return string;
	}

	public void setString(String string) {
		this.string = string;
	}

	public SerializableObject getObj() {
		return obj;
	}

	public void setObj(SerializableObject obj) {
		this.obj = obj;
	}

	class SerializableObject implements Serializable {
		private static final long serialVersionUID = 1L;
	}
}
