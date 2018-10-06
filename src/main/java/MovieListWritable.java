import org.apache.hadoop.io.LongWritable;

public class MovieListWritable extends ArrayListWritable<LongWritable> {

    public MovieListWritable() {}

    public MovieListWritable(MovieListWritable other) {
        super(other);
    }

    public void setClass() {
        setClass(LongWritable.class);
    }

}
