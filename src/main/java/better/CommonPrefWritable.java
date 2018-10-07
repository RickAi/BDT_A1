package better;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CommonPrefWritable implements WritableComparable<CommonPrefWritable> {

    LongWritable likeCount;
    LongWritable unlikeCount;

    public CommonPrefWritable() {
        likeCount = new LongWritable();
        unlikeCount = new LongWritable();
    }

    public CommonPrefWritable(LongWritable likeCount, LongWritable unlikeCount) {
        this.likeCount = likeCount;
        this.unlikeCount = unlikeCount;
    }

    public CommonPrefWritable(long likeCount, long unlikeCount) {
        this.likeCount = new LongWritable(likeCount);
        this.unlikeCount = new LongWritable(unlikeCount);
    }

    public int compareTo(CommonPrefWritable o) {
        return 0;
    }

    public void write(DataOutput dataOutput) throws IOException {
        likeCount.write(dataOutput);
        unlikeCount.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        likeCount.readFields(dataInput);
        unlikeCount.readFields(dataInput);
    }

    @Override
    public String toString() {
        return likeCount.get() + "," + unlikeCount.get();
    }
}
