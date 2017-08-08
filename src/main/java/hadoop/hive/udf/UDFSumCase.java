package hadoop.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.AbstractPrimitiveWritableObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.io.LongWritable;

import java.io.IOException;

/**
 * @Package hadoop.hive.udf
 * @Description: //todo(用一句话描述该文件做什么)
 * @Author elwyn
 * @Date 2017/8/7 22:03
 * @Email elonyong@163.com
 */
public class UDFSumCase extends AbstractGenericUDAFResolver {
    @Override
    public GenericUDAFEvaluator getEvaluator(GenericUDAFParameterInfo info) throws SemanticException {
        if (info.isAllColumns()) {
            throw new SemanticException("不支持使用*查询");

        }
        ObjectInspector[] parameterObjectInspectors = info.getParameterObjectInspectors();
        if (parameterObjectInspectors.length != 1) {
            throw new UDFArgumentException("只支持一个参数");

        }
        AbstractPrimitiveWritableObjectInspector inspector = (AbstractPrimitiveWritableObjectInspector) parameterObjectInspectors[0];
        switch (inspector.getPrimitiveCategory()) {
            case BYTE:
            case INT:
            case SHORT:
            case LONG :
                return new SumLongEvluator();
            case DOUBLE:
            case FLOAT:
            default:
                throw new UDFArgumentException("参数异常");

        }
    }



    static class SumLongEvluator extends GenericUDAFEvaluator {
        private PrimitiveObjectInspector primitiveObjectInspector;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            if (parameters.length != 1) {
                throw new UDFArgumentException("参数异常");
            }
            primitiveObjectInspector = (PrimitiveObjectInspector) parameters[0];
            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        }

        @Override
        public void close() throws IOException {
            super.close();
        }

        static class SumLongAgg implements AggregationBuffer {
            long sum;
            boolean empty;
        }


        @Override
        public void aggregate(AggregationBuffer agg, Object[] parameters) throws HiveException {

        }

        @Override
        public Object evaluate(AggregationBuffer agg) throws HiveException {
            return super.evaluate(agg);
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            SumLongAgg sal = new SumLongAgg();
            this.reset(sal);
            return sal;
        }

        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            SumLongAgg sla = (SumLongAgg) aggregationBuffer;
            sla.sum = 0;
            sla.empty = true;
        }

        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            if (objects.length != 1) {
                throw new UDFArgumentException("参数异常");
            }
            this.merge(aggregationBuffer, objects[0]);
        }

        /*
        部分聚合后的数据输出
         */
        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {

            return this.terminate(aggregationBuffer);
        }

        /**
         * 合并操作
         *
         * @param aggregationBuffer
         * @param o
         * @throws HiveException
         */
        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
            if (o != null) {
                SumLongAgg sumLongAgg = (SumLongAgg) o;
                sumLongAgg.sum += PrimitiveObjectInspectorUtils.getLong(o, primitiveObjectInspector);
                sumLongAgg.empty = false;
            }
        }

        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            SumLongAgg sumLongAgg = (SumLongAgg) aggregationBuffer;
            if (sumLongAgg.empty) {
                return null;
            }
            return new LongWritable(sumLongAgg.sum);
        }
    }
}
