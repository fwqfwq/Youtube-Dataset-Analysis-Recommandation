package MRCategoriesRanking;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RankingReducer1 extends Reducer<Text, AvgWritable, Text, FloatWritable> {

	
    @Override
    protected void reduce(Text key, Iterable<AvgWritable> values, Context context) throws IOException, InterruptedException {

        int count = 0;
        float sum = 0;
		float average = 0;

        for (AvgWritable val : values) {
            sum += val.getRate() * val.getRatings();
            count += val.getRatings();
        }

        average = sum / (float)count;
       
        context.write(key, new FloatWritable(average));
    }
}
