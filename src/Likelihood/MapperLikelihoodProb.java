package Likelihood;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class MapperLikelihoodProb extends Mapper<Object, Text, Text, Text> {

    // Bản đồ lưu tổng số lần xuất hiện của tất cả token trong từng class.
    private final Map<String, Integer> anyClassCount = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path countAnyClassDir = new Path(conf.get("countAnyClass"));
        FileSystem fs = FileSystem.get(conf); // Khởi tạo FileSystem từ config.
        FileStatus[] fileStatuses = fs.listStatus(countAnyClassDir);

        for (FileStatus status : fileStatuses) {
            Path filePath = status.getPath();
            if (status.isFile()) { 
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        // Tách dữ liệu thành cặp (class, tổng số lượng).
                        String[] kv = line.split("\t");
                        anyClassCount.put(kv[0], Integer.parseInt(kv[1])); // Lưu vào map.
                    }
                } catch (IOException e) {
                    e.printStackTrace(); 
                }
            }
        }
    }

    // Phương thức map: xử lý từng dòng dữ liệu đầu vào để tính xác suất.
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String log = value.toString();
        String[] word_classCount = log.split("\t");
        String word_class = word_classCount[0]; // Từ và class.
        String count = word_classCount[1]; 

        String Class = word_class.split(",")[1]; // Lấy class.
        Text word_classKey = new Text(word_class); // Tạo khóa (key) là từ_class.
        Text prob = new Text(); // Giá trị (value) là xác suất.

        // Duyệt qua từng class trong bản đồ anyClassCount.
        for (String keymap : anyClassCount.keySet()) {
            // Kiểm tra nếu class hiện tại trùng với class từ input.
            if (Objects.equals(keymap, Class)) {
                String countClass = anyClassCount.get(keymap).toString(); // Lấy tổng số lượng từ trong class.

                // Tính xác suất dưới dạng chuỗi "count/countClass".
                prob.set(count + "/" + countClass);

                // Ghi cặp (word_class, xác suất) vào context.
                context.write(word_classKey, prob);
            }
        }
    }
}
