package Count_Any_Class;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ReducerAnyClass extends Reducer<Text, IntWritable, Text, IntWritable> {
    private final IntWritable totalCount = new IntWritable();

    @Override
    public void reduce(Text key, Iterable<IntWritable> counts, Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable count : counts) {
            sum += count.get();
        }
        totalCount.set(sum);
        context.write(key, totalCount);
    }
}
