package MRUserSearch;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import com.google.common.base.Charsets;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnel;
import com.google.common.hash.Sink;


class UserMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
	
	Funnel<User> userF = new Funnel<User>() {
		 public void funnel(User user, Sink into) {
			 into.putString(user.user_id, Charsets.UTF_8);
		 }
	};

	BloomFilter<User> user_id = BloomFilter.create(userF, 500, 0.1);

    @Override
    protected void setup(Context context) {
    	
        Configuration conf = context.getConfiguration();
        
        String u = "VJPTbxX5Wkw";
        User user = new User(u);
        
        user_id.put(user);
    }

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String[] line = value.toString().split("\t");
        
        User user = new User(line[1]);
        if (user_id.mightContain(user)) {
            context.write(value, NullWritable.get());
        }
    }

}
