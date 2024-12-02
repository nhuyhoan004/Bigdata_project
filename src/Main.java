import Prior.MapperPrior;
import Prior.MapperPriorProb;
import Prior.ReducerPrior;
import Likelihood.MapperLikelihoodProb;
import Test.MapperEvaluation;
import Test.MapperTest;
import Test.ReducerEvaluation;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import CountAnyClass.MapperAnyClass;
import CountAnyClass.ReducerAnyClass;
import CountWordClass.MapperWordClass;
import CountWordClass.ReducerWordClass;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map;

public class Main {
    public static void main(String[] args) throws Exception {

        /*
        args[0]: file train
        args[1]: file test
        args[2]: positive class
        args[3]: negative class
        args[4]: output
        * */
        if (args.length < 5) {
            System.err.println("Usage: train_data=<train> test_data=<test> positiveClass=<positiveClass> negativeClass=<negativeClass> output");
            System.exit(2);
        }
    
        Path countWordClass = new Path("countWordClass");
        Path countAnyClass = new Path("countAnyClass");
        Path prior = new Path("prior");
        Path subprior = new Path("subprior");
        Path likelihood = new Path("likelihood");
        Path evaluation = new Path("evaluation");
        Path train_dataPath = new Path(args[0].split("=")[1]);
        Path test_dataPath = new Path(args[1].split("=")[1]);


        String positiveClass = args[2].split("=")[1];
        String negativeClass = args[3].split("=")[1];
        String allClasses = positiveClass + "," + negativeClass;

        Configuration conf = new Configuration();

        conf.set("allClasses", allClasses);
        conf.set("countWordClass", countWordClass.toString());
        conf.set("countAnyClass", countAnyClass.toString());
        conf.set("prior", prior.toString());
        conf.set("subprior", subprior.toString());
        conf.set("likelihood", likelihood.toString());
        conf.set("evaluation", evaluation.toString());

        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(countWordClass))
            fs.delete(countWordClass, true);
        if(fs.exists(countAnyClass))
            fs.delete(countAnyClass, true);
        if(fs.exists(prior))
            fs.delete(prior, true);
        if(fs.exists(subprior))
            fs.delete(subprior, true);
        if(fs.exists(likelihood))
            fs.delete(likelihood, true);
        if(fs.exists(evaluation))
            fs.delete(evaluation, true);

        Job job = Job.getInstance(conf, "Count WordClass");

        job.setJarByClass(Main.class);
        job.setMapperClass(MapperWordClass.class);
        job.setReducerClass(ReducerWordClass.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, train_dataPath);
        FileOutputFormat.setOutputPath(job, countWordClass);

        job.waitForCompletion(true);

        /*job2*/

        Job job2 = Job.getInstance(conf, "Count AnyClass");

        job2.setJarByClass(Main.class);
        job2.setMapperClass(MapperAnyClass.class);
        job2.setReducerClass(ReducerAnyClass.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job2, countWordClass);
        FileOutputFormat.setOutputPath(job2, countAnyClass);

        job2.waitForCompletion(true);

        /*job3*/
        Job job3 = Job.getInstance(conf, "Calculate Likelihood Probability");

        job3.setJarByClass(Main.class);
        job3.setMapperClass(MapperLikelihoodProb.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, countWordClass);
        FileOutputFormat.setOutputPath(job3, likelihood);

        job3.waitForCompletion(true);

        /*job4*/
        Job job4 = Job.getInstance(conf, "Count Samples");

        job4.setJarByClass(Main.class);
        job4.setMapperClass(MapperPrior.class);
        job4.setReducerClass(ReducerPrior.class);

        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job4, train_dataPath);
        FileOutputFormat.setOutputPath(job4, subprior);

        job4.waitForCompletion(true);

        Job subJob4 = Job.getInstance(conf, "Calculate Prior Probability");
        subJob4.setJarByClass(Main.class);
        subJob4.setMapperClass(MapperPriorProb.class);

        subJob4.setOutputKeyClass(Text.class);
        subJob4.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(subJob4, subprior);
        FileOutputFormat.setOutputPath(subJob4, prior);

        subJob4.waitForCompletion(true);


        /*job5*/

        Job job5 = Job.getInstance(conf, "Test");
        job5.setJarByClass(Main.class);
        job5.setMapperClass(MapperTest.class);

        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job5, test_dataPath);
        FileOutputFormat.setOutputPath(job5, new Path(args[4]));

        job5.waitForCompletion(true);

        /*Job6*/

        Job job6 = Job.getInstance(conf, "Evaluation");

        job6.setJarByClass(Main.class);
        job6.setMapperClass(MapperEvaluation.class);
        job6.setReducerClass(ReducerEvaluation.class);

        job6.setOutputKeyClass(Text.class);
        job6.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job6, new Path(args[4]));
        FileOutputFormat.setOutputPath(job6, evaluation);

        job6.waitForCompletion(true);

        Map<String, Integer> confusion_matrix = new HashMap<>();
        FileStatus[] fileStatuses = fs.listStatus(evaluation);
        for (FileStatus status : fileStatuses) {
            Path filePath = status.getPath();
            if (status.isFile()) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(filePath)))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] kv = line.split("\t");
                        confusion_matrix.put(kv[0], Integer.parseInt(kv[1]));
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        DecimalFormat decimalFormat1 = new DecimalFormat("#.#");
        DecimalFormat decimalFormat3 = new DecimalFormat("#.###");

        System.out.println("======================================");
        System.out.println("              CONFUSION MATRIX        ");
        System.out.println("======================================");
        System.out.printf("%-10s %-10s %-10s %-10s\n", "TP", "FP", "FN", "TN");
        System.out.printf("%-10d %-10d %-10d %-10d\n", 
            confusion_matrix.get("tp"), 
            confusion_matrix.get("fp"), 
            confusion_matrix.get("fn"), 
            confusion_matrix.get("tn"));
        
        int total = confusion_matrix.get("tp") +
        confusion_matrix.get("fp") +
        confusion_matrix.get("tn") +
        confusion_matrix.get("fn");
        double accuracy = (double) (confusion_matrix.get("tp") + confusion_matrix.get("tn")) / total;
        double recall = (double) confusion_matrix.get("tp") / (confusion_matrix.get("tp") + confusion_matrix.get("fn"));
        double precision = (double) confusion_matrix.get("tp") / (confusion_matrix.get("tp") + confusion_matrix.get("fp"));
        double f1_score = 2 * precision * recall / (precision + recall);

        System.out.println("\n======================================");
        System.out.println("             PERFORMANCE METRICS      ");
        System.out.println("======================================");
        System.out.printf("%-15s: %-10s\n", "ACCURACY", decimalFormat1.format(accuracy * 100) + "%");
        System.out.printf("%-15s: %-10s\n", "RECALL", decimalFormat3.format(recall));
        System.out.printf("%-15s: %-10s\n", "PRECISION", decimalFormat3.format(precision));
        System.out.printf("%-15s: %-10s\n", "F1 SCORE", decimalFormat3.format(f1_score));

    }
}
