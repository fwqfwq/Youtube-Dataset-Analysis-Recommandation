package MRTopUser;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopUserMapper extends Mapper<LongWritable, Text, CompositeKey, LongWritable> {
    
	private CompositeKey ck = new CompositeKey();
	
	private LongWritable views = new LongWritable();

	//user - view
	
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
       
    	String[] line = value.toString().split("\t");
        
        if (!line[1].isEmpty() && !line[5].isEmpty() && !line[6].isEmpty()) {
        	ck.set(line[1], line[6]);
            views = new LongWritable(Long.parseLong(line[5]));
        }

        context.write(ck, views);
    }

}
