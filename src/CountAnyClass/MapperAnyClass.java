package CountAnyClass;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MapperAnyClass extends Mapper<Object, Text, Text, IntWritable> {

    // Biến dùng để lưu khóa (class).
    private final Text Class = new Text();

    // Biến dùng để lưu giá trị đếm (số lượng).
    private final IntWritable Count = new IntWritable();

    public void map(Object classKey, Text value, Context context) throws IOException, InterruptedException {

        // Lấy một dòng từ input của countAnyClass và chuyển thành chuỗi.
        String log = value.toString();

        // Tách dòng thành hai phần: từ_class và số lượng.
        String[] word_classCount = log.split("\t");

        // Lấy từ_class (key) và số lượng (value).
        String word_class = word_classCount[0];
        String count = word_classCount[1];

        // Class
        Class.set(word_class.split(",")[1]);

        // Chuyển số lượng từ chuỗi sang số nguyên.
        Count.set(Integer.parseInt(count));

        // Ghi cặp (class, số lượng) vào context để truyền đến Reducer.
        context.write(Class, Count);
    }
}
