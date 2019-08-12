package advanced.customwritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class ForestFireReducer extends Reducer<Text, ForestFireWritable, Text, ForestFireWritable> {

    public void reduce(Text key,
                       Iterable<ForestFireWritable> values,
                       Context context) throws IOException, InterruptedException {

    }
}


