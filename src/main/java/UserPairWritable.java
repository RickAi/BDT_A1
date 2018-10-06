import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserPairWritable implements WritableComparable<UserPairWritable> {

    UserInfoWritable firstUser;
    UserInfoWritable secondUser;

    public UserPairWritable() {
        firstUser = new UserInfoWritable();
        secondUser = new UserInfoWritable();
    }

    public UserPairWritable(UserInfoWritable firstUser, UserInfoWritable secondUser) {
        this.firstUser = firstUser;
        this.secondUser = secondUser;
    }

    public void write(DataOutput dataOutput) throws IOException {
        firstUser.write(dataOutput);
        secondUser.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        firstUser.readFields(dataInput);
        secondUser.readFields(dataInput);
    }

    public double jaccardDistance(long commonLikeCount, long commonUnlikeCount) {
        long commonCount = commonLikeCount + commonUnlikeCount;
        long base = firstUser.ratingCount.get() + secondUser.ratingCount.get();
        return (double) commonCount / (base - commonCount);
    }

    @Override
    public String toString() {
        return firstUser.toString() + "-" + secondUser.toString();
    }

    public int compareTo(UserPairWritable other) {
        if (this.firstUser.getUserId() == other.firstUser.getUserId()){
            return (int) (this.secondUser.getUserId() - other.secondUser.getUserId());
        } else{
            return (int) (this.firstUser.getUserId() - other.firstUser.getUserId());
        }
    }

    @Override
    public int hashCode() {
        int res = 17;
        res = res * 31 + (int) Math.min(firstUser.getUserId(), secondUser.getUserId());
        res = res * 31 + (int) Math.max(firstUser.getUserId(), secondUser.getUserId());
        return res;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof UserPairWritable) {
            UserPairWritable otherPair = (UserPairWritable) obj;
            return (this.firstUser.equals(otherPair.firstUser)
                    && this.secondUser.equals(otherPair.secondUser))
                    || (this.firstUser.equals(otherPair.secondUser)
                    && this.secondUser.equals(otherPair.firstUser));
        }
        return false;
    }

}
