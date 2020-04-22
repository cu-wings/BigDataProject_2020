package InvertedIndex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class NewPartitioner extends HashPartitioner<Text, Text>
// org.apache.hadoop.mapreduce.lib.partition.HashPartitioner
{ // override the method
    @Override
    public int getPartition(Text key, Text value, int numReduceTasks) {
        String term = key.toString().split(",")[0]; //<term, docid>=>term
        return super.getPartition(new Text(term), value, numReduceTasks);
    }
}