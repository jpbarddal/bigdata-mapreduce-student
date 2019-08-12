package advanced.customwritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ForestFireMapper extends Mapper<Object, Text, Text, ForestFireWritable> {

    public void map(Object key, Text value, Context context) throws IOException,
            InterruptedException {


    }
}