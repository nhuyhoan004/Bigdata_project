package CountClass;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperClass extends Mapper<Object, Text, Text, IntWritable> {
    private final Text c = new Text();
    private final IntWritable count = new IntWritable();
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
        String log = value.toString();
        String[] kv = log.split("\t");
        String k = kv[0];
        String v = kv[1];

        c.set(k.split(",")[1]);
        int cnt = Integer.parseInt(v);
        count.set(cnt);
        context.write(c, count);
    }
}
