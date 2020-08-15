package MRRating;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


class RatingReducer extends Reducer<Text, MinMaxCountTuple, Text, MinMaxCountTuple> {

    private MinMaxCountTuple tuple = new MinMaxCountTuple();

    @Override
    protected void reduce(Text key, Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException {

        int sum = 0;
        
        tuple.setRate(6);  //set a larger num than 5
        tuple.setRatings(0);
        tuple.setComments(0);


        for (MinMaxCountTuple val : values) {

            if (val.getRate() < tuple.getRate()) {
            	tuple.setRate(val.getRate());
            }

            if (val.getRatings() > tuple.getRatings()) {
            	tuple.setRatings(val.getRatings());
            }

            sum += val.getComments();

        }
        tuple.setComments(sum);
        context.write(key, tuple);
    }

}
