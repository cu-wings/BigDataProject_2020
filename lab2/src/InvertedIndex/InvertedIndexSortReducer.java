package InvertedIndex;


import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexSortReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Iterator<Text> it = values.iterator();
        for (; it.hasNext(); ) {
            Text value = it.next();
            String[] s = value.toString().split(",");
            context.write(new Text(s[0]), new Text(key.toString() + "," + s[1]));
        }
    }
}