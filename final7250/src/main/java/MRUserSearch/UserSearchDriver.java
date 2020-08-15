package MRUserSearch;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class UserSearchDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		
		//Bloom Filter
    	Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "analysisUserSearch");
		
		conf.set("user_id", args[0]);
		
		//input
		job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[1]));
		
		job.setJarByClass(UserSearchDriver.class);
		job.setMapperClass(UserMapper.class);
		job.setNumReduceTasks(0);
		
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        
        //output
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[2]));
        
    	
//        //next mapreduce
//        boolean complete = job.waitForCompletion(true);
//        
//        Configuration conf2 = new Configuration();
//        Job job2 = Job.getInstance(conf2, "analysisUserInfo");
//        
//        if(complete) {
//        	
//			//inputing two files to join
//			MultipleInputs.addInputPath(job, new Path(args[3]), 
//					TextInputFormat.class, VMapper.class);
//			
//			MultipleInputs.addInputPath(job, new Path(args[4]), 
//				TextInputFormat.class, UMapper.class);
//			
//			
//			System.exit(job2.waitForCompletion(true)?0:1);
//        }
//        


        
        
        System.exit(job.waitForCompletion(true)?0:1);

    }

}
