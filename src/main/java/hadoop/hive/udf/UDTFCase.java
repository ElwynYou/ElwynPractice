package hadoop.hive.udf;

import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.*;

/**
 * @Package hadoop.hive.udf
 * @Description: //todo(用一句话描述该文件做什么)
 * @Author elwyn
 * @Date 2017/8/7 22:27
 * @Email elonyong@163.com
 */
public class UDTFCase extends GenericUDTF {

    @Override
    public void configure(MapredContext mapredContext) {
        super.configure(mapredContext);
    }

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        if (argOIs.getAllStructFieldRefs().size()!=1){
            throw new UDFArgumentException("参数异常");
        }
        ArrayList<String> fieldNames =new ArrayList<>();
        ArrayList<ObjectInspector> fieldOis =new ArrayList<>();
        fieldNames.add("id");
        fieldNames.add("name");
        fieldNames.add("price");
        fieldOis.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOis.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOis.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOis);
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] argOIs) throws UDFArgumentException {
        return super.initialize(argOIs);
    }

    @Override
    public void process(Object[] objects) throws HiveException {
            if (objects==null || objects.length!=1){
                return;
            }
            String line=objects[0].toString();
        Map<String,String> map =transforContent(line);
        List<String> string =new ArrayList<>();
        string.add(map.get("p_id"));
        string.add(map.get("p_name"));
        string.add(map.get("price"));
        super.forward(string.toArray(new String[0]));
    }

    @Override
    public void close() throws HiveException {
        super.forward(new String[]{"123456489","close","123"});
    }
    static Map<String, String> transforContent(String content) {
        Map<String, String> map = new HashMap<>();
        int i = 0;
        String key = "";
        StringTokenizer stringTokenizer = new StringTokenizer(content, "({|}|\"|:|,)");
        while (stringTokenizer.hasMoreTokens()) {
            if (++i % 2 == 0) {
                map.put(key, stringTokenizer.nextToken());
            } else {
                key = stringTokenizer.nextToken();
            }
        }
        return map;
    }
}
