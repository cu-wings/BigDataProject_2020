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
        //key <word,docid>
        //Text fileName_lineOffset = new Text(fileName +"#" + key.toString());
        //Text frequency = new Text("1");
        StringTokenizer itr = new StringTokenizer(value.toString());
        //将词和词频存入一个map中
        Map<String,Integer> map=new HashMap<>();
        for (; itr.hasMoreTokens(); ) {
            String token = itr.nextToken();
            if(!map.containsKey(token))
                map.put(token, 1);
            else
                map.replace(token, map.get(token)+1);
        }
        //遍历map获取key-value
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            word.set(entry.getKey() + "," + fileName);
            context.write(word, new Text(entry.getValue().toString()));
        }

    }
}
