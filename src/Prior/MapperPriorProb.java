package Prior;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class MapperPriorProb extends Mapper<Object, Text, Text, Text> {
    private int N = 0;
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path numberOfLines = new Path(conf.get("Nsamples"));
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatuses = fs.listStatus(numberOfLines);
        for (FileStatus status : fileStatuses) {
            Path filePath = status.getPath();
            if (status.isFile()) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] kv = line.split("\t");
                        N += Integer.parseInt(kv[1]);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] parts = line.split("\t");

        Text newValue = new Text();
        newValue.set(parts[1] + "/" + N);

        context.write(new Text(parts[0]), newValue);
    }
}

