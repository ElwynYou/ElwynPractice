package hive;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @Package hive
 * @Description: //todo(用一句话描述该文件做什么)
 * @Author elwyn
 * @Date 2017/7/13 0:14
 * @Email elonyong@163.com
 */
public class RemoveQuotesUDF extends UDF {
    public Text evaluate(Text str){
        if (null== str.toString()){
            return null;
        }
        return new Text(str.toString().replaceAll("\"",""));
    }

    public static void main(String[] args) {
        System.out.println(new RemoveQuotesUDF().evaluate(new Text("\"fdaf\"")));
    }
}
