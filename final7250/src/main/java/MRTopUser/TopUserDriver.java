package MRTopUser;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class TopUserDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		
		Configuration conf = new Configuration();
		
		
		Job job = Job.getInstance(conf, "analysisTopUser");
		
//    	//skip the bad records
//		job.getConfiguration().setInt("mapreduce.max.map.failures.percent", 10);
//		job.getConfiguration().setInt("mapreduce.max.reduce.failures.percent", 10);
		
		//input
		job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        
		job.setJarByClass(TopUserDriver.class);
		job.setMapperClass(TopUserMapper.class);
		job.setReducerClass(TopUserReducer.class);
        job.setPartitionerClass(TopUserPartitioner.class);
        job.setGroupingComparatorClass(GroupComparator.class);
		
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(LongWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        //output
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        
        
        boolean b = job.waitForCompletion(true);
        System.exit(b? 0 : 1);
	}
}
