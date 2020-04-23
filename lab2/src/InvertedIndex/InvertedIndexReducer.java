package InvertedIndex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    static Text tprev = new Text(" ");
    static List<String> postingsList = new ArrayList<String>();
    private Text word = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
/*
        Iterator<Text> it = values.iterator();
        StringBuilder all = new StringBuilder();
        //if (it.hasNext()) all.append(it.next().toString());
        for (; it.hasNext(); ) {
            Text vals = it.next();
            System.out.print("key:"+key.toString()+'\t');
            System.out.print("value:"+ vals.toString()+'\n');
        }*/
        //context.write(key, new Text(all.toString()));


        String keyWord = key.toString().split(",")[0];
        word.set(keyWord);
        String keydoc = key.toString().split(",")[1];    //key的形式为word1#doc1,所以temp为doc1
        long f = 0;
        for (Text val : values) {
            f += Long.parseLong(val.toString());
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
                out.append(doc +":"+num+ ";");
            }
            if(docnum != 0) {
                aver = (count*1.0) / docnum;
                out.insert(0, aver+",");
                context.write(tprev, new Text(out.toString()));  // Emit(tprev , P)
            }
            postingsList = new ArrayList<String>();          // P.Reset()
        }
        postingsList.add(posting.toString());         // P.Add(<n, f>)
        tprev = new Text(keyWord);                    // tprev ← t

        //System.out.print(key);
        //System.out.print(all.toString());
        //System.out.print("===============");
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
            out.append(doc +":"+num+ ";");
        }
        if(docnum != 0) {
            double aver = (count*1.0) / docnum;
            out.insert(0, aver + ",");
            context.write(tprev, new Text(out.toString()));  // Emit(tprev , P)
        }
    }
}