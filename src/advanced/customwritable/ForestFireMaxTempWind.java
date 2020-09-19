package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class ForestFireMaxTempWind {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();


        // criacao do job e seu nome
        Job j = new Job(c, "forestfire-max-vento-temp");

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class ForestFireMapper extends Mapper<Object, Text, Text, ForestFireWritable> {

        public void map(Object key, Text value, Context context) throws IOException,
                InterruptedException {


        }
    }

    public static class ForestFireReducer extends Reducer<Text, ForestFireWritable, Text, ForestFireWritable> {

        public void reduce(Text key,
                           Iterable<ForestFireWritable> values,
                           Context context) throws IOException, InterruptedException {

        }
    }


}
