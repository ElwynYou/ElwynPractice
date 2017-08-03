package hadoop.inputFormatMongodb;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Name hadoop.inputFormatMongodb.MongoDBInputSplit
 * @Description
 * @Author Elwyn
 * @Version 2017/8/2
 * @Copyright 上海云辰信息科技有限公司
 **/
public class MongoDBInputSplit extends InputSplit implements Writable {
	private long start;//起始位置,包含
	private long end;//终止位置,不包含

	public MongoDBInputSplit() {
		super();
	}

	public MongoDBInputSplit(long start, long end) {
		super();
		this.start = start;
		this.end = end;
	}

	@Override
	public long getLength() throws IOException, InterruptedException {
		return end - start;
	}

	@Override
	public String[] getLocations() throws IOException, InterruptedException {
		return new String[0];
	}

	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeLong(this.start);
		dataOutput.writeLong(this.end);
	}

	@Override
	public void readFields(DataInput dataInput) throws IOException {
		this.start = dataInput.readLong();
		this.end = dataInput.readLong();
	}

	public long getStart() {
		return start;
	}

	public void setStart(long start) {
		this.start = start;
	}

	public long getEnd() {
		return end;
	}

	public void setEnd(long end) {
		this.end = end;
	}
}
