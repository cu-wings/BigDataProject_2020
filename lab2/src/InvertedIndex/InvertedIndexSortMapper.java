package InvertedIndex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexSortMapper extends Mapper<Text, Text, Text, Text> {
    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException
    // default RecordReader: LineRecordReader; key: line offset; value: line string
    {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        //key <word,docid>
        Pattern pattern = Pattern.compile("^\\d+(\\.\\d+)?");
        Matcher matcher = pattern.matcher(value.toString());
/*
        System.out.print("key " + key.toString() + '\n');
        System.out.print("value " + value.toString() + '\n');
        System.out.print(matcher.find());
        System.out.print("====================");
*/
        String[] s = value.toString().split(",");
        context.write(new Text(matcher.group(0)), new Text(key.toString() +","+ s[1]));
    }
}