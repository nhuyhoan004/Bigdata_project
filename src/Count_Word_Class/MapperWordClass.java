package Count_Word_Class;

import Preprocess.Preprocessor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


import java.io.IOException;

import java.util.List;
import java.util.Objects;

public class MapperWordClass extends Mapper<Object, Text, Text, IntWritable>{
    private final Text word_classKey = new Text();
    private final IntWritable count = new IntWritable();
    private String[] Classes = new String[0];
    private final Preprocessor Lemmatizer = new Preprocessor();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
        Configuration conf = context.getConfiguration();
        String allClasses = conf.get("allClasses").trim();
        Classes = allClasses.split(",");
    }
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString().trim();
        String[] parts = line.split(",", 2);
        if (parts.length == 2) {
            String docClass = parts[0];
            String sentence = parts[1];

            List<String> tokens = Lemmatizer.lemmatize(sentence);

            for (String token : tokens) {
                for(String Class: Classes){
                    if (Objects.equals(Class, docClass)){
                        count.set(1);
                    }else{
                        count.set(0);
                    }
                    word_classKey.set(token + "," + Class);
                    context.write(word_classKey, count);
                }

            }
        }
    }
}
