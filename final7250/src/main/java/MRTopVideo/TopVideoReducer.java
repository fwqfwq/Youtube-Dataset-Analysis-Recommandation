package MRTopVideo;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TopVideoReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	
	private TreeMap<Double, String> tmap; 
	
	@Override
    public void setup(Context context) throws IOException, InterruptedException { 
        tmap = new TreeMap<Double, String>(); 
    } 
  
	
	@Override
	protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

		double rate = 0; 

        for (DoubleWritable val : values) {
            rate += val.get(); 
        } 
        
        tmap.put(rate, key.toString()); 
        
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
