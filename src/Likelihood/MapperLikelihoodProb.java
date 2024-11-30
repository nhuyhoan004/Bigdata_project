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
    private final Map<String, Integer> anyClassCount = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path countClassDir = new Path(conf.get("anyClassCount"));
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatuses = fs.listStatus(countClassDir);
        for (FileStatus status : fileStatuses) {
            Path filePath = status.getPath();
            if (status.isFile()) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] kv = line.split("\t");
                        anyClassCount.put(kv[0], Integer.parseInt(kv[1]));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String log = value.toString();
        String[] word_classCount = log.split("\t");
        String word_class = word_classCount[0];
        String count = word_classCount[1];

        String Class = word_class.split(",")[1];
        Text word_classKey = new Text(word_class);
        Text prob = new Text();

        for(String keymap: anyClassCount.keySet()){
            if(Objects.equals(keymap, Class)){// so sánh class của bản ghi này với keymap
                String countClass = anyClassCount.get(keymap).toString();

                prob.set(count+"/"+countClass);
                context.write(word_classKey, prob);
            }
        }
    }
}