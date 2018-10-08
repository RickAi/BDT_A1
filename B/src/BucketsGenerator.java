import java.io.*;
import java.util.*;

/**
 * Author:  RickAi
 * Email:   yongbiaoai@gmail.com
 */

public class BucketsGenerator {

    private static final boolean DEBUG = true;
    private static final String PATH_BUCKETS = "files/buckets.txt";

    // Suppose there are 10000 items, numbered 1 to 10000, and 10000 baskets, also numbered 1 to 10000
    private static final int BUCKET_SIZE = 10000;
    public static final int MAX_ITEM = BUCKET_SIZE;

    private static BucketsGenerator INSTANCE;

    private Map<Integer, List<Integer>> buckets;

    private BucketsGenerator() {}

    public static synchronized BucketsGenerator getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new BucketsGenerator();
        }
        return INSTANCE;
    }

    /**
     *  Generation rule:
     *
     *  Item i is in basket b if and only if i divides b with no remainder.
     *  Thus, item 1 is in all the baskets, item 2 is in all the even-numbered baskets, and so on.
     *  Basket 12 consists of items {1, 2, 3, 4, 6, 12}, since these are all the integers that divide 12.
     *
     */
    public Map<Integer, List<Integer>> generate() {
        if (buckets == null) {
            buckets = new HashMap<>();
            for (int curBucket = 1; curBucket <= BUCKET_SIZE; curBucket++) {
                List<Integer> integers = new ArrayList<>();
                for (int curNumber = 1; curNumber <= MAX_ITEM; curNumber++) {
                    if (curBucket % curNumber == 0) {
                        integers.add(curNumber);
                    }
                }
                buckets.put(curBucket, integers);
            }
        }

        File file = new File(PATH_BUCKETS);
        if (!file.exists() && DEBUG) {
            // save for debugging
            save(buckets, file);
        }

        return buckets;
    }

    private void save(Map<Integer, List<Integer>> buckets, File file) {
        try {
            FileOutputStream fos = new FileOutputStream(file);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
            for (Map.Entry<Integer, List<Integer>> entry : buckets.entrySet()) {
                String line = Arrays.toString(entry.getValue().toArray());
                bw.write(line);
                bw.newLine();
            }
            bw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
