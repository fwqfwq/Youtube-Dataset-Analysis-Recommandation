package MRTopUser;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompositeKey implements WritableComparable<CompositeKey> {

    public String user_id;
    public String rate;


	public String getUser_id() {
		return user_id;
	}

	public void setUser_id(String user_id) {
		this.user_id = user_id;
	}

	public String getRate() {
		return rate;
	}

	public void setRate(String rate) {
		this.rate = rate;
	}
	
	public void set(String user_id, String rate) {
		this.user_id = user_id;
		this.rate = rate;
	}


	public void write(DataOutput out) throws IOException {
		out.writeUTF(user_id);
		out.writeUTF(rate);
    }

    public void readFields(DataInput in) throws IOException {
    	user_id = in.readUTF();
    	rate = in.readUTF(); 
    }

    @Override
    public String toString() {
        return user_id + "\t" + rate + "\t";
    }

    public int compareTo(CompositeKey o) {
        if (user_id.compareTo(o.getUser_id()) < 0) return -1;
        if (user_id.compareTo(o.getUser_id()) > 0) return 1;
        return 0;
    }
}
