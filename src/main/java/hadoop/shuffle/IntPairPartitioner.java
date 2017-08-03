package hadoop.shuffle;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Package hadoop.shuffle
 * @Description: //todo(用一句话描述该文件做什么)
 * @Author elwyn
 * @Date 2017/8/2 21:39
 * @Email elonyong@163.com
 */
public class IntPairPartitioner extends Partitioner<IntPair, IntWritable> {
    public IntPairPartitioner() {
        super();
    }

    @Override
    public int getPartition(IntPair intPair, IntWritable intWritable, int i) {
        if (i >= 2) {
            int first = intPair.getFirst();
            if (first % 2 == 0) {
                //是偶数，需要第二个reducer进行处理
                return 1;
            } else {
                //是奇数，所以需要第一个reducer进行处理，返回值是从0到num-1的一个范围
                return 0;
            }
        } else {
            throw new IllegalArgumentException("reducer个数必须大于1");
        }
    }
}
