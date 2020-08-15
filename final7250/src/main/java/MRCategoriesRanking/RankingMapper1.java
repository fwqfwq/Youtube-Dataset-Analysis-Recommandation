package MRCategoriesRanking;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RankingMapper1 extends Mapper<LongWritable, Text, Text, AvgWritable> {

	//category3 - rate6/ratingg7
	
	AvgWritable avg = new AvgWritable();
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       
    	String[] line = value.toString().split("\t");
    	avg.set(Float.parseFloat(line[6]), Integer.parseInt(line[7]));
    	
    	context.write(new Text(line[3]), avg);

    }
    
}
