package hadoop.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @Package hadoop.hive.udf
 * @Description: //todo(用一句话描述该文件做什么)
 * @Author elwyn
 * @Date 2017/8/7 21:33
 * @Email elonyong@163.com
 */
public class UDFLowerUpCase extends UDF {

    public Text evalute(Text text) {
        return this.evaluate(text, "lower");
    }

    public Text evaluate(Text text, String lowerOrUpper) {
        if (text == null) {
            return text;
        }
        if ("lower".equals(lowerOrUpper)) {
            return new Text(text.toString().toLowerCase());
        } else if ("upper".equals(lowerOrUpper)) {
            return new Text(text.toString().toUpperCase());
        }
        //转换参数错误的情况下直接返回原值
        return text;

    }
}
