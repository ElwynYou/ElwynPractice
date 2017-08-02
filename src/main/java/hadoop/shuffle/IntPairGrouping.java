package hadoop.shuffle;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @Package hadoop.shuffle
 * @Description: //todo(用一句话描述该文件做什么)
 * @Author elwyn
 * @Date 2017/8/2 21:34
 * @Email elonyong@163.com
 */
public class IntPairGrouping extends WritableComparator {
    public IntPairGrouping() {
        super(IntPair.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        IntPair key1 = (IntPair) a;
        IntPair key2 = (IntPair) b;
        return Integer.compare(key1.getFirst(), key2.getFirst());
    }
}
