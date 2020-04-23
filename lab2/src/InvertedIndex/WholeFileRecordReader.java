package InvertedIndex;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class WholeFileRecordReader extends
        RecordReader<Text, Text> {
    // 用来盛放传递过来的分片
    private FileSplit fileSplit;
    private Configuration conf;
    //将作为key-value中的value值返回
    private Text currentKey = new Text();
    private Text currentValue = new Text();
    // 因为只生成一条记录，所以只需要调用一次。因此第一次调用过后将processed赋值为true，从而结束key和value的生成
    private boolean processed = false;

    /**
     * 设置RecordReader的分片和配置对象。
     */
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) split;
        this.conf = context.getConfiguration();
    }

    /**
     * 用来产生key-value值
     * 将生成的value值存入value对象中
     */
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed) {
            /**
             * 通过fileSplit找到待处理文件，然后再读入内容到value中
             */
            //byte[] contents = new byte[(int) fileSplit.getLength()];
            Path file = fileSplit.getPath();
            FileSystem fs = file.getFileSystem(conf);
            FSDataInputStream in = fs.open(file);
            String line = "";
            String total = "";
            BufferedReader br = new BufferedReader(new InputStreamReader(in));
            while ((line = br.readLine()) != null) {
                total = total + line + "\n";
            }
            br.close();
            in.close();
            fs.close();
            currentValue = new Text(total);
            processed = true;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException,
            InterruptedException {
        return currentKey;
    }

    @Override
    public Text getCurrentValue() throws IOException,
            InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return processed ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {
        //do nothing
    }

}