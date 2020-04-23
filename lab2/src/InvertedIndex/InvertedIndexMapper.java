package InvertedIndex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException
// default RecordReader: LineRecordReader; key: line offset; value: line string
    {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        Text word = new Text();
        String[] s = fileName.split("\\.");
        fileName = fileName.substring(0, fileName.length() - 14);
        //key <word,docid>
        StringTokenizer itr = new StringTokenizer(value.toString());
        //将词和词频存入一个map中
        Map<String, Integer> map = new HashMap<>();
        for (; itr.hasMoreTokens(); ) {
            String token = itr.nextToken();
            if (!map.containsKey(token))
                map.put(token, 1);
            else
                map.replace(token, map.get(token) + 1);
        }
        //遍历map获取key-value
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            word.set(entry.getKey() + "," + fileName);
            context.write(word, new Text(entry.getValue().toString()));
            System.out.print("key:"+word.toString()+'\t');
            System.out.print("value:"+entry.getValue().toString()+'\n');
        }
    }
}
