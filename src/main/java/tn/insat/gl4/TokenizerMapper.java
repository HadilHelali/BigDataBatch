package tn.insat.app;


import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class TokenizerMapper
        extends Mapper<Object, Text, Text, FloatWritable>{

    private FloatWritable price = new FloatWritable();
    private Text magazin = new Text();

    public void map(Object key, Text value, Mapper.Context context
    ) throws IOException, InterruptedException {
        String[] itr = value.toString().split("\t");
        System.out.println(itr[2]);
        magazin.set(itr[2]);
        System.out.println(itr[4]);
        price.set(Float.parseFloat(itr[4]));
        context.write(magazin, price);

    }
}