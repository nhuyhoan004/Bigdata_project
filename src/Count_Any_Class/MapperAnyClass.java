package Count_Any_Class;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperAnyClass extends Mapper<Object, Text, Text, IntWritable> {
    private final Text Class = new Text();
    private final IntWritable Count = new IntWritable();
    public void map(Object classKey, Text value, Context context) throws IOException, InterruptedException{
        String log = value.toString();
        String[] word_classCount = log.split("\t");
        String word_class = word_classCount[0];
        String count = word_classCount[1];

        Class.set(word_class.split(",")[1]);
        Count.set(Integer.parseInt(count));
        context.write(Class, Count);
    }
}
