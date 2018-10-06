import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;

public abstract class ArrayListWritable<M extends Writable> extends ArrayList<M> implements Writable, Configurable {
    private static final long serialVersionUID = 1L;
    private Class<M> refClass = null;
    private Configuration conf;

    public ArrayListWritable() {
    }

    public ArrayListWritable(ArrayListWritable<M> arrayListWritable) {
        super(arrayListWritable);
    }

    public ArrayListWritable(Class<M> refClass) {
        this.refClass = refClass;
    }

    public void setClass(Class<M> refClass) {
        if (this.refClass != null) {
            throw new RuntimeException("setClass: refClass is already set to " + this.refClass.getName());
        } else {
            this.refClass = refClass;
        }
    }

    public abstract void setClass();

    public void readFields(DataInput in) throws IOException {
        if (this.refClass == null) {
            this.setClass();
        }

        this.clear();
        int numValues = in.readInt();
        this.ensureCapacity(numValues);

        for(int i = 0; i < numValues; ++i) {
            M value = (M) ReflectionUtils.newInstance(this.refClass, this.conf);
            value.readFields(in);
            this.add(value);
        }

    }

    public void write(DataOutput out) throws IOException {
        int numValues = this.size();
        out.writeInt(numValues);

        for(int i = 0; i < numValues; ++i) {
            ((Writable)this.get(i)).write(out);
        }

    }

    public final Configuration getConf() {
        return this.conf;
    }

    public final void setConf(Configuration conf) {
        this.conf = conf;
    }
}