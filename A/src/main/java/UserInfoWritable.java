import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserInfoWritable implements WritableComparable<UserInfoWritable> {

    LongWritable userId;
    LongWritable ratingCount;

    public UserInfoWritable() {
        userId = new LongWritable();
        ratingCount = new LongWritable();
    }

    public UserInfoWritable(LongWritable userId, LongWritable ratingCount) {
        this.userId = userId;
        this.ratingCount = ratingCount;
    }

    public UserInfoWritable(long userId, long ratingCount) {
        this.userId = new LongWritable(userId);
        this.ratingCount = new LongWritable(ratingCount);
    }

    public UserInfoWritable(UserInfoWritable other) {
        this.userId = new LongWritable(other.userId.get());
        this.ratingCount = new LongWritable(other.ratingCount.get());
    }

    public int compareTo(UserInfoWritable o) {
        return this.userId.compareTo(o.userId);
    }

    public void write(DataOutput dataOutput) throws IOException {
        userId.write(dataOutput);
        ratingCount.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        userId.readFields(dataInput);
        ratingCount.readFields(dataInput);
    }

    @Override
    public String toString() {
        return userId.toString();
    }

    public long getUserId() {
        return this.userId.get();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof UserInfoWritable) {
            UserInfoWritable other = (UserInfoWritable) obj;
            return this.userId.equals(other.userId);
        }
        return false;
    }
}
