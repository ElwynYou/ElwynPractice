package hadoop.inputFormatMongodb;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Name hadoop.inputFormatMongodb.PersonMongoDBWriteable
 * @Description
 * @Author Elwyn
 * @Version 2017/8/2
 * @Copyright 上海云辰信息科技有限公司
 **/
public class PersonMongoDBWriteable implements MongoDBWritable {
	private String name;
	private Integer age;
	private String sex = "";
	private int count = 0;

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeUTF(this.name);
		dataOutput.writeUTF(this.sex);
		if (this.age == null) {
			dataOutput.writeBoolean(false);
		} else {
			dataOutput.writeBoolean(true);
			dataOutput.writeInt(this.age);
		}
		dataOutput.writeInt(this.count);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.name = dataInput.readUTF();
		this.sex = dataInput.readUTF();
		if (dataInput.readBoolean()) {
			this.age = dataInput.readInt();
		} else {
			this.age = null;
		}
		this.count = dataInput.readInt();
	}

	@Override
	public void readFields(Document object) {
		this.name = object.getString("name");
		if (object.get("age") != null) {
			this.age = object.getInteger("age");
			this.count=1;
		} else {
			this.age = null;
		}
	}

	@Override
	public void write(MongoCollection mongoCollection) {
		Document document=new Document();
		document.append("age", this.age).append("count", this.count);
		mongoCollection.insertOne(document);

	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public Integer getAge() {
		return age;
	}

	public void setAge(Integer age) {
		this.age = age;
	}

	public String getSex() {
		return sex;
	}

	public void setSex(String sex) {
		this.sex = sex;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}
}
