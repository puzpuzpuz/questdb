/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2022 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

#include <cstdint>
#include "util.h"
#include "jni.h"
#include <atomic>

#define COUNTER_T uint16_t

template<typename T>
void set_max_atomic(std::atomic<T> &slot, T value) {
    do {
        T current = slot.load();
        if (value <= current || slot.compare_exchange_strong(current, value, std::memory_order_acq_rel)) {
            break;
        }
    } while (true);
}

const int64_t MIN_VERSION_MASK = 0xFFFF;

struct min_version_pair {
    // first 2 bytes used for update version
    // and last 6 bytes for minimum of scoreboard
    int64_t min_version;

    inline min_version_pair(const int64_t &min, const uint16_t &version) {
        this->min_version = (min << 16) | (int64_t) version; // TODO: check with uint16_t overflow
    }

    [[nodiscard]] inline int64_t get_min() const {
        return min_version >> 16;
    }

    [[nodiscard]] inline uint16_t get_version() const {
        return (uint16_t )(min_version & MIN_VERSION_MASK);
    }
};

template<typename T>
class txn_scoreboard_t {
    uint32_t mask = 0;
    uint32_t size = 0;
    std::atomic<int64_t> max = 0;
    // 1-based min txn that is in-use
    // we have to use 1 based txn to rule out possibility of 0 txn
    // 0 is initial value when shared memory is created

    // Note(puzpuzpuz): Split min and min version
    // std::atomic<int64_t> min = 0;
    // std::atomic<int64_t> min_version = 0;
    std::atomic<min_version_pair> min_version{min_version_pair(L_MAX, 0)};
    std::atomic<T> counts[];

    inline static T inc(T val) {
        return val + 1;
    }

    inline static T dec(T val) {
        return val - 1;
    }

    inline std::atomic<T> &_get_count(const int64_t offset) {
        return counts[offset & mask];
    }

    inline bool increment_count(int64_t txn) {
        auto curr_min_version = min_version.load(std::memory_order_acquire);
        auto cmin_version = min_version.load();

        while (true) {
            auto current_min = curr_min_version.get_min();
            if (current_min > txn) {
                return false;
            }
            _get_count(txn).fetch_add(1, std::memory_order_acq_rel);

            min_version_pair updated_min_version {current_min, curr_min_version.get_version() + 1};
            // Note(puzpuzpuz): we don't really need a CAS here. It's enough to check that the min version didn't change;
            //                  We could use same approach as in seqlock here: whoever does update_min() is the writer.
            //if (!min_version.compare_exchange_strong(curr_min_version, updated_min_version)) {
            auto cmin_version_new = min_version.load();
            if (cmin_version_new != cmin_version) {
                // roll back
                auto c = _get_count(txn).fetch_add(-1, std::memory_order_acq_rel);
                // Note(puzpuzpuz): we may need it or not
                if (c == 0) {
                    update_min();
                }
                return false;
            } else {
                return true;
            }
        }
    }

    inline void update_min(const int64_t txn) {
        auto cmin_version = min_version.load();
        // Note(puzpuzpuz): update min_version to write intent value (odd)
        if (!cas(min_version)) {
            // someone else succeeded - just exit
            return;
        }

        auto current_min = min_version.load(std::memory_order_acquire);
        auto new_min = current_min.get_min();
        while (new_min < txn) {
            while (new_min < txn && get_count_unchecked(new_min) == 0) {
                new_min++;
            }

            if (current_min.get_min() == new_min
                || min_version.compare_exchange_strong(
                    current_min,
                    min_version_pair{new_min, current_min.get_version() + 1},
                    std::memory_order_acq_rel)) {
                // Note(puzpuzpuz): update min_version to write finished value (even)
                return;
            }
            new_min = current_min.get_min();
        }
    }

public:

    inline int64_t get_min() {
        return min_version.load().get_min();
    }

    inline int64_t get_max() {
        return max.load();
    }

    inline T get_count(int64_t offset) {
        if (offset < get_min()) {
            // Note(puzpuzpuz): would be nice to have a meaningful comment here.
            return 0;
        }
        return get_count_unchecked(offset);
    }

    inline T get_count_unchecked(const int64_t offset) {
        return _get_count(offset).load();
    }

    inline void txn_release(int64_t txn) {
        // Note(puzpuzpuz): we could move this thing into the if body
        const int64_t max_offset = get_max();
        // this is atomic decrement
        if (_get_count(txn).fetch_add(-1) == 0 && get_min() == txn) {
            // Note(puzpuzpuz): ideally this should be the only place where we call update_min()
            update_min(max_offset);
        }
    }

    inline int32_t txn_acquire(int64_t txn) {
        // Note(puzpuzpuz): we may need to read the min value and initialize it with a CAS if it's the default one here

        int64_t _min = get_min();
        if (txn < _min) {
            return -2;
        }

        if (txn - _min >= size) {
            update_min(txn);
        }

        if (txn - get_min() < size) {
            if (!increment_count(txn)) {
                // Race lost, someone updated min to higher value. Roll back the increment.
                return -2;
            }
            update_min(txn);
            set_max_atomic(max, txn);
            return 0;
        }
        return -1;
    }


    void init(uint32_t entry_count) {
        mask = entry_count - 1;
        size = entry_count;
    }
};

extern "C" {

JNIEXPORT jint JNICALL Java_io_questdb_cairo_TxnScoreboard_acquireTxn0
        (JAVA_STATIC, jlong p_txn_scoreboard, jlong txn) {
    return reinterpret_cast<txn_scoreboard_t<COUNTER_T> *>(p_txn_scoreboard)->txn_acquire(txn);
}

JNIEXPORT void JNICALL Java_io_questdb_cairo_TxnScoreboard_releaseTxn0
        (JAVA_STATIC, jlong p_txn_scoreboard, jlong txn) {
    reinterpret_cast<txn_scoreboard_t<COUNTER_T> *>(p_txn_scoreboard)->txn_release(txn);
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_getCount
        (JAVA_STATIC, jlong p_txn_scoreboard, jlong txn) {
    return (jlong) (reinterpret_cast<txn_scoreboard_t<COUNTER_T> *>(p_txn_scoreboard))->get_count(txn);
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_getMin
        (JAVA_STATIC, jlong p_txn_scoreboard) {
    return reinterpret_cast<txn_scoreboard_t<COUNTER_T> *>(p_txn_scoreboard)->get_min();
}

JNIEXPORT jlong JNICALL Java_io_questdb_cairo_TxnScoreboard_getScoreboardSize
        (JAVA_STATIC, jlong entryCount) {
    return sizeof(txn_scoreboard_t<COUNTER_T>) + entryCount * sizeof(std::atomic<COUNTER_T>);
}

JNIEXPORT void JNICALL Java_io_questdb_cairo_TxnScoreboard_init
        (JAVA_STATIC, jlong p_txn_scoreboard, jlong entryCount) {
    reinterpret_cast<txn_scoreboard_t<COUNTER_T> *>(p_txn_scoreboard)->init(entryCount);
}

}
