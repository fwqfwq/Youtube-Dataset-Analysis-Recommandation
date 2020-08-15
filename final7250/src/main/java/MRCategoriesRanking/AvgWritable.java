package MRCategoriesRanking;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class AvgWritable implements Writable {
	
	private float rate;
	private int ratings; // number of ratings
//	private float avg = 0;
	
	
	AvgWritable() { }
	
	
	public float getRate() {
		return rate;
	}

	public void setRate(float rate) {
		this.rate = rate;
	}

	public int getRatings() {
		return ratings;
	}

	public void setRatings(int ratings) {
		this.ratings = ratings;
	}

	public void set(float rate, int ratings) {
		this.rate = rate;
		this.ratings = ratings;
	}
	
	public void readFields(DataInput in) throws IOException {
		ratings = in.readInt();
		rate = in.readFloat();
		
	}
	
	public void write(DataOutput out) throws IOException {
		out.writeInt(ratings);
		out.writeFloat(rate);
	}
	

}
