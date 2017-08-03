package hadoop.inputFormatMongodb;

import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Name hadoop.inputFormatMongodb.NullMongoDBWritable
 * @Description
 * @Author Elwyn
 * @Version 2017/8/2
 * @Copyright 上海云辰信息科技有限公司
 **/
public class NullMongoDBWritable implements MongoDBWritable {
	@Override
	public void readFields(Document object) {

	}

	@Override
	public void write(MongoCollection mongoCollection) {

	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {

	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {

	}
}
