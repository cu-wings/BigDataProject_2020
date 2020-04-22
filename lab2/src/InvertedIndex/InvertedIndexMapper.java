package InvertedIndex;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
// default RecordReader: LineRecordReader; key: line offset; value: line string
    {
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        Text word = new Text();
        Text fileName_lineOffset = new Text(fileName +"#" + key.toString());
        StringTokenizer itr = new StringTokenizer(value.toString());
        for (; itr.hasMoreTokens(); ) {
            word.set(itr.nextToken());
            context.write(word, fileName_lineOffset);
        }
        //System.out.print(key);
        
        //System.out.print("===============");
    }
}
