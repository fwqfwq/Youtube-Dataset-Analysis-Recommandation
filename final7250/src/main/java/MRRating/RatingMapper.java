package MRRating;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


class RatingMapper extends Mapper<Object, Text, Text, MinMaxCountTuple> {

	//user1 - rate6 / ratings7 / comments8
	private MinMaxCountTuple tuple = new MinMaxCountTuple();

    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        String[] line = value.toString().split("\t");

        tuple.setRate(Float.valueOf(line[6]));
        tuple.setRatings(Float.valueOf(line[7]));
        tuple.setComments(Float.valueOf(line[8]));

        context.write(new Text(line[1]), tuple);
    }

}
