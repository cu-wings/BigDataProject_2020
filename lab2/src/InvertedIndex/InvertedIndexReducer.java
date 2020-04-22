package InvertedIndex;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Iterator<Text> it = values.iterator();
        StringBuilder all = new StringBuilder();
        if (it.hasNext()) all.append(it.next().toString());
        for (; it.hasNext(); ) {
            all.append(";");
            all.append(it.next().toString());
        }
        context.write(key, new Text(all.toString()));
        //System.out.print(key);
        //System.out.print(all.toString());
        //System.out.print("===============");
    } //最终输出键值对示例：(“fish", “doc1#0; doc1#8;doc2#0;doc2#8 ")
}