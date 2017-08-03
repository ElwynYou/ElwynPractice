package hadoop.mapreduce.maxTemperature;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 */
public class MaxTemperature {
    public static void main(String[] args)throws Exception {
        if (args.length !=2){
            System.err.println("Usage: MaxTemperature <input path> <output path>");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        //指定文件系统的路径
        conf.set("fs.defaultFS","hdfs://hadoop-master.nebuinfo.com:8020");
        //让mapreduce运行在yarn上
        conf.set("mapreduce.framework.name", "yarn");
        //如果要运行在yarn上必须把jar包位置告诉yarn
        conf.set("mapreduce.job.jar", "F:\\WorkSpace\\ElwynPractice\\out\\artifacts\\ElwynPractice_jar\\ElwynPractice.jar");
        //配置yarn的resourcemanager在哪台机器
        conf.set("yarn.resourcemanager.hostname", "hadoop-master.nebuinfo.com");
        //启用跨平台
        conf.set("mapreduce.app-submission.cross-platform", "true");
        /**
         *  org.apache.hadoop.security.AccessControlException: Permission denied:
         *  以上异常是因为 window的用户(一般是administrator) 没有操作hdfs文件系统的权限,
         *  需要在开发工具配置vm选项-DHADOOP_USER_NAME=[hdfs的用户]
         *  或者可以加入System.setProperty("HADOOP_USER_NAME", "[hdfs的用户]");
         */
        Job job=Job.getInstance(conf,"Max temperature");
        job.setJarByClass(MaxTemperature.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setMapperClass(MapTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);
        job.setCombinerClass(MaxTemperatureReducer.class);
        job.setOutputKeyClass(Text.class);              //注1
        job.setOutputValueClass(IntWritable.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);   
    }
}
