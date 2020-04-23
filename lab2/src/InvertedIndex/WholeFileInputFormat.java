package InvertedIndex;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class WholeFileInputFormat extends FileInputFormat<Text,Text> {

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;//单个文件不允许再切片
    }

    @Override
    public RecordReader<Text, Text> createRecordReader(InputSplit arg0, TaskAttemptContext arg1) throws IOException,
            InterruptedException {
        RecordReader<Text,Text> recordReader = new WholeFileRecordReader();
        return recordReader;
    }

}