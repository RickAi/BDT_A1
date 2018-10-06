package old;

import org.apache.hadoop.io.*;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class UserInfoWritable implements WritableComparable<UserInfoWritable> {

    LongWritable userId;
    MovieListWritable likeMovies;
    MovieListWritable unlikeMovies;

    public UserInfoWritable() {
        userId = new LongWritable();
        likeMovies = new MovieListWritable();
        unlikeMovies = new MovieListWritable();
    }

    public UserInfoWritable(UserInfoWritable other) {
        userId = new LongWritable(other.userId.get());
        likeMovies = new MovieListWritable(other.getLikeMovies());
        unlikeMovies = new MovieListWritable(other.getUnlikeMovies());
    }

    public void write(DataOutput dataOutput) throws IOException {
        userId.write(dataOutput);
        likeMovies.write(dataOutput);
        unlikeMovies.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        userId.readFields(dataInput);
        likeMovies.readFields(dataInput);
        unlikeMovies.readFields(dataInput);
    }

    public void setUserId(LongWritable userId) {
        this.userId = userId;
    }

    public void setLikeMovies(MovieListWritable likeMovies) {
        this.likeMovies = likeMovies;
    }

    public void setUnlikeMovies(MovieListWritable unlikeMovies) {
        this.unlikeMovies = unlikeMovies;
    }

    public MovieListWritable getLikeMovies() {
        return likeMovies;
    }

    public MovieListWritable getUnlikeMovies() {
        return unlikeMovies;
    }

    /**
     * A = curUser.Like
     * B = otherUser.Like
     * C = curUser.Unlike
     * D = otherUser.Unlike
     *
     * Jaccard distance:
     *
     *          (A ^ B) + (C ^ D)
     * -----------------------------------
     * (A V B V C V D) - (A ^ B) - (C ^ D)
     *
     * @param otherUser
     * @return
     */
    public DoubleWritable jaccardDistance(UserInfoWritable otherUser) {
        long likeInterCount = intersectCount(this.likeMovies, otherUser.likeMovies);
        long unlikeInterCount = intersectCount(this.unlikeMovies, otherUser.unlikeMovies);
        long totalInterCount = likeInterCount + unlikeInterCount;
        long base = this.likeMovies.size() + otherUser.likeMovies.size()
                + this.unlikeMovies.size() + otherUser.unlikeMovies.size() - totalInterCount;
        return new DoubleWritable((double) totalInterCount / base);
    }

    /**
     * Assume preprocessed movie id list have been sorted
     *
     * @param curMovies
     * @param otherMovies
     * @return
     */
    private long intersectCount(MovieListWritable curMovies, MovieListWritable otherMovies) {
        long count = 0l;
        for (int curIndex = 0, otherIndex = 0;
             curIndex < curMovies.size() && otherIndex < otherMovies.size(); ) {
            if (curMovies.get(curIndex).get() == otherMovies.get(otherIndex).get()) {
                count++;
                otherIndex++;
                curIndex++;
            } else if (curMovies.get(curIndex).get() > otherMovies.get(otherIndex).get()) {
                otherIndex++;
            } else {
                curIndex++;
            }
        }
        return count;
    }

    public int compareTo(UserInfoWritable o) {
        return this.userId.compareTo(o.userId);
    }

}
