package MRCategoriesRanking;

import java.io.IOException;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RankingMapper2  extends Mapper<LongWritable, Text, FloatWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

    	
        String line[] = value.toString().split("\t");

        try {
            FloatWritable f = new FloatWritable(Float.parseFloat(line[1]));
            context.write(f, new Text(line[0]));

        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    
    

    
}




