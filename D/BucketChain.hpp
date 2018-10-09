
//
// Created by aiyongbiao on 2018/10/9.
//

#ifndef D_BUCKETCHAIN_HPP
#define D_BUCKETCHAIN_HPP

#endif //D_BUCKETCHAIN_HPP

#include <iostream>
#include <vector>
#include "BucketPair.hpp"
#include <math.h>

class BucketChain {

public:

    BucketChain(int range) {
        range_ = range;
        int size = (int) ceil(log(range / 2) / log(2));
        for (int i = 0; i < size; i++) {
            BucketPair pair;
            bucket_pairs_.push_back(pair);
        }
    }

    int query() {
        int count = 0, size = bucket_pairs_.size();
        BucketPair last_pair;
        int last_level = 0;
        for (int index = size - 1; index >= 0; index--) {
            int level = size - index - 1;
            BucketPair &pair = bucket_pairs_[index];
            if (pair.validBucketCount() > 0) {
                last_pair = pair;
                last_level = level;
                count += pair.getValidCount(level);
            }
        }
        if (last_pair.validBucketCount() == 1) {
            count -= last_pair.getValidCount(last_level) / 2;
        } else {
            count -= last_pair.getValidCount(last_level) / 4;
        }
        return count;
    }

    void update(int timestamp, int bit) {
        deleteOldBuckets(timestamp);
        int stamp = timestamp; // TODO: the stamp should after mod
        if (bit == 0) {
            return;
        }

        for (int index = bucket_pairs_.size() - 1; index >= 0; index--) {
            BucketPair &pair = bucket_pairs_[index];
            if (pair.validBucketCount() >= 2) {
                stamp = pair.reset(stamp);
            } else {
                // no need for further update
                pair.update(stamp);
                break;
            }
        }
    }

    void deleteOldBuckets(int timestamp) {
        for (BucketPair &pair : bucket_pairs_) {
            if (pair.validBucketCount() > 0) {
                if (pair.isExpired(timestamp, range_)) {
                    pair.toInValid(timestamp, range_);
                }
                break;
            }
        }
    }

    std::vector<BucketPair> bucket_pairs_;
    int range_;

};