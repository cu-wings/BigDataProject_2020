package InvertedIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;


public class InvertedIndexer {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();

            //conf.set("fs.defaultFS", "hdfs://localhost:9000");
            //指定输入输出目录
            final String OUTPUT_PATH = args[1];
            Path path = new Path(OUTPUT_PATH);

            //加载配置文件
            FileSystem fileSystem = path.getFileSystem(conf);

            //输出目录若存在则删除
            if (fileSystem.exists(new Path(OUTPUT_PATH))) {
                fileSystem.delete(new Path(OUTPUT_PATH), true);
            }


            Job job = Job.getInstance(conf, "invert index");
            job.setJarByClass(InvertedIndexer.class);
            job.setInputFormatClass(WholeFileInputFormat.class);

            job.setOutputKeyClass(Text.class);  //指定输出的key的类型，Text相当于String类
            job.setOutputValueClass(Text.class);  //指定输出的Value的类型，Text相当于String类

            job.setPartitionerClass(NewPartitioner.class);
            job.setMapperClass(InvertedIndexMapper.class);
            job.setReducerClass(InvertedIndexReducer.class);

            Path out1path = new Path(path+"/inverted");
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, out1path);

            //System.exit(job.waitForCompletion(true) ? 0 : 1);


            Job job2 = Job.getInstance(conf, "invert index sorted");
            job2.setInputFormatClass(KeyValueTextInputFormat.class);
            job2.setJarByClass(InvertedIndexer.class);

            job2.setOutputKeyClass(Text.class);  //指定输出的key的类型，Text相当于String类
            job2.setOutputValueClass(Text.class);  //指定输出的Value的类型，Text相当于String类

            job2.setMapperClass(InvertedIndexSortMapper.class);
            job2.setReducerClass(InvertedIndexSortReducer.class);

            FileInputFormat.setInputPaths(job2, out1path);
            FileOutputFormat.setOutputPath(job2, new Path(path + "/sorted"));

            if(job.waitForCompletion(true)){
                System.exit(job2.waitForCompletion(true) ? 0 : 1);
            };

            //System.exit(job2.waitForCompletion(true) ? 0 : 1);


        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
