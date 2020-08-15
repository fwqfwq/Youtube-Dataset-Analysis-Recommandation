package MRRating;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;




public class RatingDriver {


    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "analysisRatingMinMax");
		
		//input
		job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        
		job.setJarByClass(RatingDriver.class);
		job.setMapperClass(RatingMapper.class);
		job.setCombinerClass(RatingReducer.class);
		job.setReducerClass(RatingReducer.class);
		
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MinMaxCountTuple.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MinMaxCountTuple.class);
        
        

        //output
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        
        System.exit(job.waitForCompletion(true)?0:1);
        

    }

}
