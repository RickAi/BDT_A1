import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserPrefWritable implements WritableComparable<UserPrefWritable> {

    UserInfoWritable userInfo;
    BooleanWritable like;

    public UserPrefWritable() {
        userInfo = new UserInfoWritable();
        like = new BooleanWritable();
    }

    public UserPrefWritable(UserPrefWritable other) {
        this.userInfo = new UserInfoWritable(other.userInfo);
        this.like = new BooleanWritable(other.like.get());
    }

    public UserPrefWritable(UserInfoWritable userInfo, BooleanWritable like) {
        this.userInfo = userInfo;
        this.like = like;
    }

    public int compareTo(UserPrefWritable o) {
        return this.userInfo.compareTo(o.userInfo);
    }

    public void write(DataOutput dataOutput) throws IOException {
        userInfo.write(dataOutput);
        like.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        userInfo.readFields(dataInput);
        like.readFields(dataInput);
    }

}
