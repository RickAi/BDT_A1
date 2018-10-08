import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Author:  RickAi
 * Email:   yongbiaoai@gmail.com
 */

public class FrequentSetFinder {

    private Map<Integer, List<Integer>> srcBuckets;

    public FrequentSetFinder(Map<Integer, List<Integer>> srcBuckets) {
        this.srcBuckets = srcBuckets;
    }

    public void setSrcBuckets(Map<Integer, List<Integer>> srcBuckets) {
        this.srcBuckets = srcBuckets;
    }

    public List<ItemSet> findFrequentItemList(int supportThreshold) {
        Map<Integer, List<ItemSet>> frequentSets = findFrequentSets(supportThreshold);
        List<ItemSet> res = new ArrayList<>();

        for (Map.Entry<Integer, List<ItemSet>> entry : frequentSets.entrySet()) {
            res.addAll(entry.getValue());
        }
        return res;
    }

    private Map<Integer, List<ItemSet>> findFrequentSets(int supportThreshold) {
        Map<Integer, List<ItemSet>> itemSetsMap = new HashMap<>();

        // init first iteration with base elements
        // format: 5, 89 etc.
        Map<Integer, Integer> baseElements = new HashMap<>();
        for (Map.Entry<Integer, List<Integer>> entry : srcBuckets.entrySet()) {
            List<Integer> bucketElements = entry.getValue();
            for (Integer integer : bucketElements) {
                Integer value = baseElements.get(integer);
                if (value == null) {
                    baseElements.put(integer, 1);
                } else {
                    baseElements.put(integer, value + 1);
                }
            }
        }

        // filter base itemset with threshold
        List<ItemSet> baseItemSets = new ArrayList<>();
        for (Map.Entry<Integer, Integer> entry : baseElements.entrySet()) {
            if (entry.getValue() >= supportThreshold) {
                ItemSet item = new ItemSet(entry.getKey(), entry.getValue());
                baseItemSets.add(item);
            }
        }
        itemSetsMap.put(1, baseItemSets);

        List<ItemSet> preItemSets = baseItemSets;
        int curIteration = 1;
        while (!preItemSets.isEmpty()) {
            curIteration++;

            List<ItemSet> candidateSets = buildCandidateItemSets(preItemSets);
            for (Map.Entry<Integer, List<Integer>> entry : srcBuckets.entrySet()) {
                List<Integer> bucket = entry.getValue();
                for (ItemSet item : candidateSets) {
                    if (bucket.size() >= item.size() && contains(bucket, item)) {
                        item.increaseFrequent();
                    }
                }
            }

            List<ItemSet> curItemSets = new ArrayList<>();
            for (ItemSet item : candidateSets) {
                if (item.getOccurFrequent() >= supportThreshold) {
                    curItemSets.add(item);
                }
            }

            if (!curItemSets.isEmpty()) {
                itemSetsMap.put(curIteration, curItemSets);
            }
            preItemSets = curItemSets;
        }

        return itemSetsMap;
    }

    public List<ItemSet> findLargestSet(int supportThreshold) {
        Map<Integer, List<ItemSet>> frequentSets = findFrequentSets(supportThreshold);
        return frequentSets.get(frequentSets.size() - 1);
    }

    // build next iteration item sets based on the prev sets
    private List<ItemSet> buildCandidateItemSets(List<ItemSet> preItemSets) {
        List<ItemSet> res = new ArrayList<ItemSet>();

        // loop-loop item sets in the last iteration
        for (ItemSet curSet : preItemSets) {
            for (ItemSet otherSet : preItemSets) {
                if (curSet != otherSet && curSet.canJoin(otherSet)) {
                    ItemSet union = new ItemSet();
                    union.addAll(curSet);
                    union.add(otherSet.get(otherSet.size() - 1));

                    boolean missSubSet = false;
                    List<ItemSet> subItemSets = union.buildSubItemSets();
                    for (ItemSet itemSet : subItemSets) {
                        if (!preItemSets.contains(itemSet)) {
                            missSubSet = true;
                            break;
                        }
                    }
                    if (!missSubSet) {
                        res.add(union);
                    }
                }
            }
        }
        return res;
    }

    private boolean contains(List<Integer> bucket, List<Integer> item) {
        int bucketIndex = 0, itemIndex = 0;
        for (; bucketIndex < bucket.size() && itemIndex < item.size();) {
            if (bucket.get(bucketIndex).compareTo(item.get(itemIndex)) == 0) {
                bucketIndex++;
                itemIndex++;
            } else if (bucket.get(bucketIndex).compareTo(item.get(itemIndex)) > 0) {
                return false;
            } else {
                bucketIndex++;
            }
        }
        return itemIndex == item.size();
    }

}
