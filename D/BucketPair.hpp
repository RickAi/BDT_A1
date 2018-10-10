
//
// Created by aiyongbiao on 2018/10/9.
//

#ifndef D_BUCKETPAIR_HPP
#define D_BUCKETPAIR_HPP

#endif //D_BUCKETPAIR_HPP

#include "Bucket.hpp"
#include <iostream>
#include <vector>
#include <math.h>

class BucketPair {

public:

    BucketPair() {
        for (int i = 0; i < 2; i++) {
            Bucket bucket;
            buckets_.push_back(bucket);
        }
    }

    int getValidCount(int level) {
        int count = 0;
        for (Bucket &bucket : buckets_) {
            count += bucket.valid() ? pow(2, level) : 0;
        }
        return count;
    }

    int validBucketCount() {
        int count = 0;
        for (Bucket &bucket : buckets_) {
            count += bucket.valid() ? 1 : 0;
        }
        return count;
    }

    void update(int timestamp) {
        for (Bucket &bucket : buckets_) {
            if (!bucket.valid()) {
                bucket.end_timestamp_ = timestamp;
                break;
            }
        }
    }

    int reset(int timestamp) {
        int old = 0;
        for (Bucket &bucket : buckets_) {
            old = std::max(bucket.end_timestamp_, old);
            bucket.toInValid();
        }
        buckets_[0].end_timestamp_ = timestamp;
        return old;
    }

    bool isExpired(int timestamp, int range) {
        for (Bucket &bucket : buckets_) {
            // TODO: the way should be optimized
            if (timestamp - bucket.end_timestamp_ >= range) {
                return true;
            }
        }
    }

    void toInValid(int timestamp, int range) {
        for (Bucket &bucket : buckets_) {
            if (timestamp - bucket.end_timestamp_>= range) {
                bucket.toInValid();
            }
        }
    }

    void print(int level) {
        for (Bucket &bucket : buckets_) {
            if (bucket.valid()) {
                std::cout << pow(2, level) << " ";
            }
        }
    }

    std::vector<Bucket> buckets_;

};