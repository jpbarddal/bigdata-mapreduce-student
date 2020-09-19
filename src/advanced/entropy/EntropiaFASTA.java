package advanced.entropy;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;


public class EntropiaFASTA {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        // Definir codigo padrao de rotinas mapreduce
        BasicConfigurator.configure();

        Configuration c = new Configuration();

        // arquivo de entrada
        Path input = new Path("in/JY157487.1.fasta");

        // arquivo temporario
        Path intermediate = new Path("intermediario.txt");

        // arquivo de saida
        Path output = new Path("output/entropia.txt");

    }



}
