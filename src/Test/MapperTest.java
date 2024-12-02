package Test;

import Preprocess.Preprocessor;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MapperTest extends Mapper<Object, Text, Text, Text> {
    private final Map<String, String> likelihood = new HashMap<>();
    private final Map<String, String> prior = new HashMap<>();
    private final Map<String, String> countAnyClass = new HashMap<>();
    private String[] Classes = new String[0];
    private final Preprocessor Lemmatizer = new Preprocessor();
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();

        Path priorDir = new Path(conf.get("prior"));
        Path likelihoodDir = new Path(conf.get("likelihood"));
        Path countAnyClassDir = new Path(conf.get("countAnyClass"));

        FileSystem fs = FileSystem.get(conf);

        FileStatus[] fileStatusesPrior = fs.listStatus(priorDir);
        for (FileStatus status : fileStatusesPrior) {
            Path filePath = status.getPath();
            if (status.isFile()) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] kv = line.split("\t");
                        prior.put(kv[0], kv[1]);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        FileStatus[] fileStatusesLikelihood = fs.listStatus(likelihoodDir);
        for (FileStatus status : fileStatusesLikelihood) {
            Path filePath = status.getPath();
            if (status.isFile()) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] kv = line.split("\t");
                        likelihood.put(kv[0], kv[1]);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        FileStatus[] fileStatusesCountClass = fs.listStatus(countAnyClassDir);
        for (FileStatus status : fileStatusesCountClass) {
            Path filePath = status.getPath();
            if (status.isFile()) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] kv = line.split("\t");
                        countAnyClass.put(kv[0], kv[1]);
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        String allClasses = conf.get("allClasses").trim();
        Classes = allClasses.split(",");
    }

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString().trim();
        String[] parts = line.split(",",2);

        String document = parts[1];
        String referenceClass = parts[0];

        List<String> tokens = Lemmatizer.lemmatize(document);

        String predictClass = Classes[0];
        double probability = -1e9;
        Map<String, Double> class_probability = new HashMap<>();
        for (String c: Classes){
            double sumLogLikelihood = 0;
            double logPrior;
            double p;

            for (String token: tokens){
                if (likelihood.containsKey(token+","+c)) {
                    String[] xy = likelihood.get(token + "," + c).split("/");
                    double x = Double.parseDouble(xy[0]);
                    double y = Double.parseDouble(xy[1]);
                    sumLogLikelihood += Math.log(x / y);
                }else{
                    String countAnyClass_c = countAnyClass.get(c);
                    double y = Double.parseDouble(countAnyClass_c);
                    sumLogLikelihood += Math.log(1 / y);
                }
            }

            String[] xy = prior.get(c).split("/");
            double x = Double.parseDouble(xy[0]);
            double y = Double.parseDouble(xy[1]);
            logPrior = Math.log(x / y);

            p = logPrior + sumLogLikelihood;
            class_probability.put(c,p);

            if(p > probability){
                predictClass = c;
                probability = p;
            }
        }
//        probability = Math.exp(probability);
        double z = 0;
        for (String c: class_probability.keySet()){
            z += Math.exp(class_probability.get(c));
        }
        for (String c: class_probability.keySet()){
            class_probability.put(c, Math.exp(class_probability.get(c)) / z);
        }

        StringBuilder sb = new StringBuilder();

        for (Map.Entry<String, Double> entry : class_probability.entrySet()) {
            sb.append(entry.getKey())
                    .append(":")
                    .append(entry.getValue())
                    .append(",");
        }

        if (!class_probability.isEmpty()) {
            sb.setLength(sb.length() - 1); // Xóa dấu phẩy
        }

        String mapString = sb.toString();

        mapString = !Objects.equals(referenceClass, "") ? referenceClass + "\t" + mapString : mapString;

        Text newKey = new Text(document);
        Text newValue = new Text(mapString);
        context.write(newKey, newValue);

    }
}
