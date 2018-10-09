
//
// Created by aiyongbiao on 2018/10/9.
//

#ifndef D_BUCKET_HPP
#define D_BUCKET_HPP

#endif //D_BUCKET_HPP

class Bucket {

public:

    Bucket() {
        end_timestamp_ = INVALID_STAMP;
    }

    bool valid() {
        return end_timestamp_ != INVALID_STAMP;
    }

    void toInValid() {
        end_timestamp_ = INVALID_STAMP;
    }

    int end_timestamp_;

private:
    int INVALID_STAMP = -1;

};