package com;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * mapper
 * input:<key: line offset，value：line string>
 * output:<key: word1#doc1,value: 1>
 *
 * partitioner
 * 只根据word哈希
 *
 * Combiner
 * input:<key: word1#doc1,value: 1>
 * output:<key: word1#doc1,value: sum>
 *
 * reducer
 * intput:<key: word1#doc1,value: sum>
 * output:<key: word1，value：<doc1,count>;<..>;...>
 *
 **/

public class InvertedIndex {
    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, IntWritable> {
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            String filename2 = new String();
            String temp = new String();
            int dot = filename.lastIndexOf('.');
            temp = filename.substring(0,dot);
            dot = temp.lastIndexOf('.');
            filename2 = filename.substring(0, dot);

            Text word = new Text();
            StringTokenizer itr = new StringTokenizer(value.toString());
            for(; itr.hasMoreTokens(); ) {
                word.set(itr.nextToken()+"#"+filename2);
                context.write(word, new IntWritable(1));
            }
        }
    }
    public static class NewCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count += val.get();
            }
            result.set(count);
            context.write(key, result);
        }
    }

    public static class NewPartitioner extends HashPartitioner<Text, IntWritable> {
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String term = new String();
            term = key.toString().split("#")[0]; // <term#docid>=>term
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, IntWritable, Text, Text>{
        static Text tprev = new Text(" ");
        static List<String> postingsList = new ArrayList<String>();
        private Text word = new Text();
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String keyWord = key.toString().split("#")[0];
            word.set(keyWord);
            String keydoc = key.toString().split("#")[1];    //key的形式为word1#doc1,所以temp为doc1
            long f = 0;
            for (IntWritable val : values) {
                f += val.get();
            }

            String posting = new String(keydoc+","+f);
            if(!tprev.equals(word) && !tprev.equals(" ")){              //if t ≠ tprev ^ tprev ≠ Ø then
                StringBuilder out = new StringBuilder();  //字符串变量
                long count = 0;
                long docnum = 0;
                double aver = 0;
                for (String p : postingsList) {
                    long num =  Long.parseLong(p.substring(p.indexOf(",") + 1));
                    String doc = p.substring(0,p.indexOf(","));
                    count = count + num; //该单词总数
                    docnum = docnum + 1; //文档数
                    out.append(doc +":"+num+ "；");
                }
                if(docnum != 0) {
                    aver = (count*1.0) / docnum;
                    out.insert(0, aver+"，");
                    context.write(tprev, new Text(out.toString()));  // Emit(tprev , P)
                }
                postingsList = new ArrayList<String>();          // P.Reset()
            }
            postingsList.add(posting.toString());         // P.Add(<n, f>)
            tprev = new Text(keyWord);                    // tprev ← t
        }

        //将最后一个单词的key-value输出
        public void cleanup(Context context) throws IOException,InterruptedException {
            StringBuilder out = new StringBuilder();
            long count = 0;
            long docnum = 0;
            for (String p : postingsList) {
                long num =  Long.parseLong(p.substring(p.indexOf(",") + 1));
                String doc = p.substring(0,p.indexOf(","));
                count = count + num;
                docnum = docnum + 1;
                out.append(doc +":"+num+ "；");
            }
            if(docnum != 0) {
                double aver = (count*1.0) / docnum;
                out.insert(0, aver + "，");
                context.write(tprev, new Text(out.toString()));  // Emit(tprev , P)
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "inverted index");     //新建一个用户定义的Job
        //job.addCacheFile(new URI("hdfs://localhost:9000/input/People_List_unique.txt"));

        job.setJarByClass(InvertedIndex.class);   //设置执行任务的jar

        job.setMapperClass(InvertedIndexMapper.class);
        job.setCombinerClass(NewCombiner.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setPartitionerClass(NewPartitioner.class);

        job.setMapOutputKeyClass(Text.class);  
        job.setMapOutputValueClass(IntWritable.class);  
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


