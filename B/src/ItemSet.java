import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Author:  RickAi
 * Email:   yongbiaoai@gmail.com
 */

public class ItemSet extends ArrayList<Integer> {

    private int occurFrequent;

    public ItemSet() {}

    public ItemSet(int baseElement, int occurFrequent) {
        this.add(baseElement);
        this.occurFrequent = occurFrequent;
    }

    public void increaseFrequent() {
        this.occurFrequent += 1;
    }

    public int getOccurFrequent() {
        return occurFrequent;
    }

    public void resetFrequent() {
        this.occurFrequent = 0;
    }

    // build the sub item sets based on current set
    public List<ItemSet> buildSubItemSets() {
        List<ItemSet> ret = new ArrayList<ItemSet>();
        if (size() > 1) {
            for (Integer curInt : this) {
                ItemSet subSet = new ItemSet();
                subSet.addAll(this);
                subSet.remove(curInt);
                ret.add(subSet);
            }
        }
        return ret;
    }

    public boolean canJoin(ItemSet other) {
        if (other.size() != this.size()) {
            return false;
        }

        Iterator<Integer> it1 = this.iterator();
        Iterator<Integer> it2 = other.iterator();
        while (it1.hasNext()) {
            Integer item1 = it1.next();
            Integer item2 = it2.next();
            int result = item1.compareTo(item2);
            if (result != 0) {
                if (it1.hasNext())
                    return false;
                return result < 0;
            }
        }
        return false;
    }

    @Override
    public String toString() {
        return Arrays.toString(this.toArray());
    }
}
