package old;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserPairWritable implements WritableComparable<UserPairWritable> {

    private LongWritable firstUserId;
    private LongWritable secondUserId;

    public UserPairWritable() {
        firstUserId = new LongWritable();
        secondUserId = new LongWritable();
    }

    public UserPairWritable(UserPairWritable otherPair) {
        firstUserId = new LongWritable(otherPair.firstUserId.get());
        secondUserId = new LongWritable(otherPair.secondUserId.get());
    }

    public UserPairWritable(LongWritable firstUserId, LongWritable secondUserId) {
        this.firstUserId = firstUserId;
        this.secondUserId = secondUserId;
    }

    public void write(DataOutput dataOutput) throws IOException {
        firstUserId.write(dataOutput);
        secondUserId.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        firstUserId.readFields(dataInput);
        secondUserId.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        int res = 17;
        res = res * 31 + (int) Math.min(firstUserId.get(), secondUserId.get());
        res = res * 31 + (int) Math.max(firstUserId.get(), secondUserId.get());
        return res;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof UserPairWritable) {
            UserPairWritable otherPair = (UserPairWritable) obj;
            return (this.firstUserId.equals(otherPair.firstUserId)
                    && this.secondUserId.equals(otherPair.secondUserId))
                    || (this.firstUserId.equals(otherPair.secondUserId)
                    && this.secondUserId.equals(otherPair.firstUserId));
        }
        return false;
    }

    public int compareTo(UserPairWritable other) {
        if (this.firstUserId.get() == other.firstUserId.get()){
            return (int) (this.secondUserId.get() - other.secondUserId.get());
        } else{
            return (int) (this.firstUserId.get() - other.firstUserId.get());
        }
    }

    @Override
    public String toString() {
        return this.firstUserId.toString() + "," + this.secondUserId.toString();
    }
}
