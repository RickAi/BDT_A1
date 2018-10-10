#include <iostream>
#include <fstream>
#include "BucketChain.hpp"
#include <map>

int main() {
    std::fstream mock_stream("../stream.txt", std::fstream::in);

    std::map<int, int> debug_map; // debug only

    char upcoming_bit;
    int timestamp = 0;
    BucketChain chain(1000);
    while (mock_stream >> upcoming_bit) {
        timestamp++;

        chain.update(timestamp, (upcoming_bit == '1') ? 1 : 0);

        // --- debug only
        if (upcoming_bit == '1') {
            debug_map[timestamp / 1000] += 1;
        }
        // ---

        if (timestamp % 1000 == 0) {
            std::cout << "Number of ones in last 1000-bit stream:(" << chain.query() << ", " << debug_map[timestamp / 1000 - 1] << ")/" << timestamp << std::endl;
            chain.print();
        }
    }

    mock_stream.close();

    return 0;
}