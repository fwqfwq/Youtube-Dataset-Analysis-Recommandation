package MRRating;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;


public class MinMaxCountTuple implements Writable {

    private float rate;
    private float ratings;
    private float comments;


    public float getRate() {
		return rate;
	}

	public void setRate(float rate) {
		this.rate = rate;
	}

	public float getRatings() {
		return ratings;
	}

	public void setRatings(float ratings) {
		this.ratings = ratings;
	}

	public float getComments() {
		return comments;
	}

	public void setComments(float comments) {
		this.comments = comments;
	}
	

    public void write(DataOutput out) throws IOException {
        out.writeFloat(rate);
        out.writeFloat(ratings);
        out.writeFloat(comments);

    }

    public void readFields(DataInput in) throws IOException {
        rate = in.readFloat();
        ratings = in.readFloat();
        comments = in.readFloat();
    }

    @Override
    public String toString() {
        return (rate + "\t" + ratings + "\t" + comments);
    }

}
