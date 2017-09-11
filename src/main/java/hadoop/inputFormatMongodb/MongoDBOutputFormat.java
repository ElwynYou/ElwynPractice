package hadoop.inputFormatMongodb;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;

/**
 * @Package hadoop.inputFormatMongodb
 * @Description: //自定义outputformat
 * @Author elwyn
 * @Date 2017/8/1 23:43
 * @Email elonyong@163.com
 */
public class MongoDBOutputFormat<V extends MongoDBWritable> extends OutputFormat<NullWritable, V> {
	public MongoDBOutputFormat() {
		super();
	}

//这个方法返回的RecordWriter将告诉我们数据怎么输出到输出文件里
	@Override
	public RecordWriter<NullWritable, V> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		return new MongoDBRecordWriter<>(taskAttemptContext);
	}

	//验证输出路径是否存在
	@Override
	public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

	}

	/**
	 * 二：GetOutputCommitter：获取一个OutPutCommitter对象主要负责：
	 1在job初始化的时候生成一些配置信息，临时输出文件夹等
	 2.在job完成的时候处理一些工作
	 3.配置task 临时文件
	 4.调度task任务的提交
	 5.提交task输出文件
	 6.取消task的文件的提交
	 * @param taskAttemptContext
	 * @return
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	public OutputCommitter getOutputCommitter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
		return new FileOutputCommitter(null, taskAttemptContext);
	}
}
