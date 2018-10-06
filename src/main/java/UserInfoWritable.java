import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserInfoWritable implements WritableComparable<UserInfoWritable> {

    LongWritable userId;
    LongWritable likeCount;
    LongWritable unlikeCount;

    public UserInfoWritable() {
        userId = new LongWritable();
        likeCount = new LongWritable();
        unlikeCount = new LongWritable();
    }

    public UserInfoWritable(LongWritable userId, LongWritable likeCount, LongWritable unlikeCount) {
        this.userId = userId;
        this.likeCount = likeCount;
        this.unlikeCount = unlikeCount;
    }

    public UserInfoWritable(long userId, long likeCount, long unlikeCount) {
        this.userId = new LongWritable(userId);
        this.likeCount = new LongWritable(likeCount);
        this.unlikeCount = new LongWritable(unlikeCount);
    }

    public UserInfoWritable(UserInfoWritable other) {
        this.userId = new LongWritable(other.userId.get());
        this.likeCount = new LongWritable(other.likeCount.get());
        this.unlikeCount = new LongWritable(other.unlikeCount.get());
    }

    public int compareTo(UserInfoWritable o) {
        return this.userId.compareTo(o.userId);
    }

    public void write(DataOutput dataOutput) throws IOException {
        userId.write(dataOutput);
        likeCount.write(dataOutput);
        unlikeCount.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        userId.readFields(dataInput);
        likeCount.readFields(dataInput);
        unlikeCount.readFields(dataInput);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(userId.get()).append(",").append(likeCount.get()).append(",").append(unlikeCount.get());
        return sb.toString();
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
