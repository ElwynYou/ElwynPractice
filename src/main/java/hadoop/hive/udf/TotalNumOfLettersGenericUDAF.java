package hadoop.hive.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFParameterInfo;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import java.io.IOException;

/**
 * @Name hadoop.hive.udf.TotalNumOfLettersGenericUDAF
 * @Description
 * @Author Elwyn
 * @Version 2017/8/8
 * @Copyright 上海云辰信息科技有限公司
 **/
@Description(name = "letters", value = "_FUNC_(expr) - 返回该列中所有字符串的字符总数")
public class TotalNumOfLettersGenericUDAF extends AbstractGenericUDAFResolver {


	@Override
	public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
		if (parameters.length!=1){
			throw new UDFArgumentTypeException(parameters.length-1,"Exactly on argument is expected");

		}

		ObjectInspector oi = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(parameters[0]);
		//判断类型
		if(oi.getCategory()!=ObjectInspector.Category.PRIMITIVE){
			throw new UDFArgumentTypeException(0,
					"Argument must be PRIMITIVE, but "
							+ oi.getCategory().name()
							+ " was passed.");
		}
		PrimitiveObjectInspector inputOI = (PrimitiveObjectInspector) oi;

		if (inputOI.getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING){
			throw new UDFArgumentTypeException(0,
					"Argument must be String, but "
							+ inputOI.getPrimitiveCategory().name()
							+ " was passed.");
		}
		return new TotalNumOfLettersEvaluator();
	}
	public static class TotalNumOfLettersEvaluator extends GenericUDAFEvaluator {

		PrimitiveObjectInspector inputOI;
		ObjectInspector outputOI;
		PrimitiveObjectInspector integerOI;

		int total = 0;
		// 确定各个阶段输入输出参数的数据格式ObjectInspectors
		@Override
		public ObjectInspector init(Mode m, ObjectInspector[] parameters)
				throws HiveException {

			assert (parameters.length == 1);
			super.init(m, parameters);

			//map阶段读取sql列，输入为String基础数据格式
			// init方法中根据不同的mode指定输出数据的格式objectinspector
			if (m == Mode.PARTIAL1 || m == Mode.COMPLETE) {
				inputOI = (PrimitiveObjectInspector) parameters[0];
			} else {
				//其余阶段，输入为Integer基础数据格式
				integerOI = (PrimitiveObjectInspector) parameters[0];
			}
			// 不同model阶段的输出数据格式
			// 指定各个阶段输出数据格式都为Integer类型
			outputOI = ObjectInspectorFactory.getReflectionObjectInspector(Integer.class,
					ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
			return outputOI;

		}

		/**
		 * 存储当前字符总数的类
		 */
		static class LetterSumAgg implements AggregationBuffer {
			int sum = 0;
			void add(int num){
				sum += num;
			}
		}
		// 保存数据聚集结果的类
		@Override
		public AggregationBuffer getNewAggregationBuffer() throws HiveException {
			LetterSumAgg result = new LetterSumAgg();
			return result;
		}

		// 重置聚集结果
		@Override
		public void reset(AggregationBuffer agg) throws HiveException {
			LetterSumAgg myagg = new LetterSumAgg();
		}

		private boolean warned = false;

		// map阶段，迭代处理输入sql传过来的列数据
		@Override
		public void iterate(AggregationBuffer agg, Object[] parameters)
				throws HiveException {
			assert (parameters.length == 1);
			if (parameters[0] != null) {
				LetterSumAgg myagg = (LetterSumAgg) agg;
				Object p1 = ((PrimitiveObjectInspector) inputOI).getPrimitiveJavaObject(parameters[0]);
				myagg.add(String.valueOf(p1).length());
			}
		}

		// map与combiner结束返回结果，得到部分数据聚集结果
		@Override
		public Object terminatePartial(AggregationBuffer agg) throws HiveException {
			LetterSumAgg myagg = (LetterSumAgg) agg;
			total += myagg.sum;
			return total;
		}
		// combiner合并map返回的结果，还有reducer合并mapper或combiner返回的结果。
		@Override
		public void merge(AggregationBuffer agg, Object partial)
				throws HiveException {
			if (partial != null) {

				LetterSumAgg myagg1 = (LetterSumAgg) agg;

				Integer partialSum = (Integer) integerOI.getPrimitiveJavaObject(partial);

				LetterSumAgg myagg2 = new LetterSumAgg();

				myagg2.add(partialSum);
				myagg1.add(myagg2.sum);
			}
		}

		// reducer阶段，输出最终结果
		@Override
		public Object terminate(AggregationBuffer agg) throws HiveException {
			LetterSumAgg myagg = (LetterSumAgg) agg;
			total = myagg.sum;
			return myagg.sum;
		}

	}
}
