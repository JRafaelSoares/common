//  Copyright 2019 U.C. Berkeley RISE Lab
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

#ifndef INCLUDE_LATTICES_MULTI_KEY_SNAPSHOT_ISOLATION_LATTICE_HPP
#define INCLUDE_LATTICES_MULTI_KEY_SNAPSHOT_ISOLATION_LATTICE_HPP

#include "core_lattices.hpp"
#include <google/protobuf/util/time_util.h>
#include <iostream>
const time_t minSITimeStamp = 0;
template <typename T>
struct SnapshotIsolationVersion {
    time_t snapshot;
    T value;

    SnapshotIsolationVersion<T>(){
        snapshot = minSITimeStamp;
        value = T();
    }

    // need this because of static cast
    SnapshotIsolationVersion<T>(unsigned) {
        snapshot = minSITimeStamp;
        value = T();
    }

    SnapshotIsolationVersion<T>(time_t timestamp, T v) {
        snapshot = timestamp;
        value = v;
    }

    unsigned size() {
        return sizeof(time_t) + value.size();
    }
};
template <typename T>
struct SnapshotIsolationPayload {
    time_t snapshot;
    std::map<time_t, SnapshotIsolationVersion<T>, std::greater<time_t>> previous_versions;
    T value;

    SnapshotIsolationPayload<T>() {
        snapshot = minSITimeStamp;
        value = T();
    }

  // need this because of static cast
  SnapshotIsolationPayload<T>(unsigned) {
      snapshot = minSITimeStamp;
      value = T();
  }

    SnapshotIsolationPayload<T>(time_t timestamp, T v,
                                        std::map<time_t, SnapshotIsolationVersion<T>> prev_vers) {
        snapshot = timestamp;
        previous_versions = prev_vers;
        value = v;
    }

    // For versions only
    SnapshotIsolationPayload<T>(time_t timestamp, T v) {
        snapshot = timestamp;
        value = v;
    }

  unsigned size() {
    int size_of_map = 0;
    if(previous_versions.size() != 0){
        for(auto element : previous_versions){
            size_of_map = previous_versions.size() * element.second.size();
            break;
        }
    }
    return sizeof(time_t) + value.size() + sizeof(std::map<time_t, SnapshotIsolationVersion<T>>) + size_of_map;
  }
};

template <typename T>
class SnapshotIsolationLattice : public Lattice<SnapshotIsolationPayload<T>> {
 protected:
  void do_merge(const SnapshotIsolationPayload<T> &p) {
        // Current version is more recent
        if (this->element.snapshot >= p.snapshot){
            if (p.snapshot != minSITimeStamp){
                for (auto const& version: p.previous_versions){
                    // We assume no two versions of the same object can have the same timestamp
                    this->element.previous_versions[version.first] = version.second;
                }
                SnapshotIsolationVersion<T> copy(p.snapshot, p.value);
                this->element.previous_versions[p.snapshot] = copy;
            }
        }
        else{ // We got a newer version. Insert ourselves in the older versions and update values
            // We dont want to add versions which are the "base" lattice
            if(this->element.snapshot != minSITimeStamp){
                SnapshotIsolationVersion<T> copy(this->element.snapshot, this->element.value);
                this->element.previous_versions[this->element.snapshot] = copy;
            }
            this->element.snapshot = p.snapshot;
            this->element.value = p.value;
            for (auto const& version: p.previous_versions){
                // We assume no two versions of the same object can have the same timestamp
                this->element.previous_versions[version.first] = version.second;
            }
        }
  }

 public:
    SnapshotIsolationLattice() :
      Lattice<SnapshotIsolationPayload<T>>(SnapshotIsolationPayload<T>()) {}
    SnapshotIsolationLattice(const SnapshotIsolationPayload<T> &p) :
      Lattice<SnapshotIsolationPayload<T>>(p) {}
  MaxLattice<unsigned> size() { return {this->element.size()}; }
};


#endif  // INCLUDE_LATTICES_MULTI_KEY_SNAPSHOT_ISOLATION_LATTICE_HPP
