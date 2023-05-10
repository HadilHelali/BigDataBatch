package tn.insat.gl4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TokenizerMapper
        extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text type = new Text();

    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {

        String[] itr = value.toString().split(",");
        System.out.println(itr[2]);
        if (itr[25] != "")
            type.set(itr[25]);
        context.write(type, one);
    }
}