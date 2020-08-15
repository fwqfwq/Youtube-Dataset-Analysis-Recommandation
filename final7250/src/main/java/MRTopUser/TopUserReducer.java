package MRTopUser;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class TopUserReducer extends Reducer<CompositeKey, LongWritable, Text, LongWritable> {
	
	private LongWritable result = new LongWritable();
	
	@Override
	protected void reduce(CompositeKey key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

        long max_views = 0;
        
        for (LongWritable val : values) {
            if(val.get() > max_views) max_views = val.get();
        }

        result.set(max_views);
        context.write(new Text(key.toString()), result);
    }

}
