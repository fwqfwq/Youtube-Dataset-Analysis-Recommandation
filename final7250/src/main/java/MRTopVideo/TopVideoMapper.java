package MRTopVideo;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TopVideoMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	private TreeMap<Double, String> tmap; 
	
	@Override
    public void setup(Context context) throws IOException, InterruptedException { 
        tmap = new TreeMap<Double, String>(); 
    } 
  
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	
    	String[] line = value.toString().split("\t");
    	  
    	//video - rate
        String video = line[0]; 
        double rate = Double.parseDouble(line[6]); 

        tmap.put(rate, video); 
        
    	if (tmap.size() > 10) {
            tmap.remove(tmap.firstKey()); 
        }
    }
    
	@Override
    public void cleanup(Context context) throws IOException, InterruptedException {
  
        for (Map.Entry<Double, String> entry : tmap.entrySet()) {
        	double r = entry.getKey(); 
            String v = entry.getValue(); 
            
            context.write(new Text(v), new DoubleWritable(r)); 
        } 
    }	
}
