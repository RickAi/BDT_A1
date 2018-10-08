import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.util.List;
import java.util.Map;

/**
 * Author:  RickAi
 * Email:   yongbiaoai@gmail.com
 */

public class Main {

    private static final String PATH_A = "A.txt";
    private static final String PATH_B = "B.txt";

    public static void main(String[] args) {
        Question1();

        Question2();
    }

    private static void Question1() {
        System.out.println("If the support threshold is 100, which items are frequent?");

        Map<Integer, List<Integer>> buckets = BucketsGenerator.getInstance().generate();
        FrequentSetFinder finder = new FrequentSetFinder(buckets);
        List<ItemSet> frequentItemList = finder.findFrequentItemList(8);

        for (ItemSet item : frequentItemList) {
            System.out.println(item.getOccurFrequent() + ":" + item);
        }

        saveItems(frequentItemList, PATH_A);
    }

    private static void Question2() {
        System.out.println("\nIf the support threshold is 20, find the maximal frequent itemsets.");

        Map<Integer, List<Integer>> buckets = BucketsGenerator.getInstance().generate();
        FrequentSetFinder finder = new FrequentSetFinder(buckets);
        List<ItemSet> largestSet = finder.findLargestSet(3);
        for (ItemSet item : largestSet) {
            System.out.println(item.getOccurFrequent() + ":" + item);
        }

        saveItems(largestSet, PATH_B);
    }

    private static void saveItems(List<ItemSet> itemSets, String dstPath) {
        File file = new File(dstPath);
        try {
            FileOutputStream fos = new FileOutputStream(file, false);
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
            for (ItemSet item : itemSets) {
                bw.write(item.toString());
                bw.newLine();
            }
            bw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
