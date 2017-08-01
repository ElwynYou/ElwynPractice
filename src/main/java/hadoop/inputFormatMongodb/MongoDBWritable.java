package hadoop.inputFormatMongodb;

import com.mongodb.client.MongoCollection;
import org.apache.hadoop.io.Writable;
import org.bson.Document;

/**
 * @Package hadoop.inputFormatMongodb
 * @Description: //todo(用一句话描述该文件做什么)
 * @Author elwyn
 * @Date 2017/8/1 23:31
 * @Email elonyong@163.com
 */
public interface MongoDBWritable extends Writable {
    /**
     * 从mongodb中读取数据
     *
     * @param object
     */
    void readFields(Document object);

    /**
     * 往mongodb中写入数据
     *
     * @param mongoCollection
     */
    void write(MongoCollection mongoCollection);
}
