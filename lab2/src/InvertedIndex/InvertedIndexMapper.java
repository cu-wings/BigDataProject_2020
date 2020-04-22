package InvertedIndex;

import java.io.IOException;
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
        Text fileName_lineOffset = new Text(fileName +"#" + key.toString());
        Text frequency = new Text("1");
        StringTokenizer itr = new StringTokenizer(value.toString());
        for (; itr.hasMoreTokens(); ) {
            word.set(itr.nextToken() + "," + fileName);
            context.write(word, frequency);
        }
        //System.out.print(key);
        
        //System.out.print("===============");
    }
}
