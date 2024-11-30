package CountWordClass;

import Preprocess.Preprocessor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

public class MapperWordClass extends Mapper<Object, Text, Text, IntWritable> {

    // Biến dùng để lưu khóa (key) của từng từ kèm class.
    private final Text word_classKey = new Text();

    // Biến dùng để lưu giá trị đếm (value).
    private final IntWritable count = new IntWritable();

    // Mảng lưu danh sách các class.
    private String[] Classes = new String[0];

    // Đối tượng dùng để tiền xử lý (lemmatization) câu.
    private final Preprocessor Lemmatizer = new Preprocessor();

    // Phương thức init: chạy một lần trước khi xử lý dữ liệu để khởi tạo cấu hình.
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        String allClasses = conf.get("allClasses").trim(); // Lấy giá trị danh sách class.
        Classes = allClasses.split(",");
    }

    // Phương thức map: xử lý từng dòng dữ liệu đầu vào.
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] parts = line.split(",", 2); // Tách dòng thành 2 phần: class và câu.

        // Kiểm tra nếu dữ liệu có đủ 2 phần: class và câu.
        if (parts.length == 2) {
            String cclass = parts[0]; // Lấy class của dòng.
            String sentence = parts[1]; // Lấy câu của dòng.

            // Thực hiện lemmatization trên câu để lấy danh sách token.
            List<String> tokens = Lemmatizer.lemmatize(sentence);

            for (String token : tokens) {
                for (String Class : Classes) {
                    // Kiểm tra nếu class hiện tại trùng với class của dòng.
                    if (Objects.equals(Class, cclass)) {
                        count.set(1); // Gán giá trị đếm là 1.
                    } else {
                        count.set(0); // Gán giá trị đếm là 0.
                    }

                    // Tạo khóa theo định dạng "token,class".
                    word_classKey.set(token + "," + Class);

                    // Ghi cặp (key, value) vào context.
                    context.write(word_classKey, count);
                }
            }
        }
    }
}
