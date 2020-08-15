package MRCategoriesRanking;

import java.io.IOException;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class RankingDriver {
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "analysisCategoriesRanking");
		
		//input
		job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));
        
		job.setJarByClass(RankingDriver.class);
		job.setMapperClass(RankingMapper1.class);
		job.setReducerClass(RankingReducer1.class);
		
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(AvgWritable.class);
        
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FloatWritable.class);
        
        

        //output
        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));
        
//        System.exit(job.waitForCompletion(true)?0:1);
        
        
      //next mapreduce
        boolean complete = job.waitForCompletion(true);
        
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "analysisCategoriesRanking");
        if(complete) {
        	
    		//input
    		job2.setInputFormatClass(TextInputFormat.class);
            TextInputFormat.addInputPath(job2, new Path(args[2]));

    		job2.setJarByClass(RankingDriver.class);
    		job2.setMapperClass(RankingMapper2.class);
    		job2.setReducerClass(RankingReducer2.class);
    		
            job2.setMapOutputKeyClass(FloatWritable.class);
            job2.setMapOutputValueClass(Text.class);

            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(FloatWritable.class);

            //sortComparator
            job2.setSortComparatorClass(SortKeyComparator.class);

            job2.setNumReduceTasks(1);

            //output
            job2.setOutputFormatClass(TextOutputFormat.class);
            TextOutputFormat.setOutputPath(job2, new Path(args[3]));

            System.exit(job2.waitForCompletion(true)?0:1);
            
        }

	}
}
