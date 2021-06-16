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
const uint64_t minSITimeStamp = 0;

template <typename T>
struct SnapshotIsolationPayload {
    uint64_t snapshot;
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

    SnapshotIsolationPayload<T>(uint64_t timestamp) {
        snapshot = timestamp;
        value = T();
    }

    SnapshotIsolationPayload<T>(uint64_t timestamp, T v) {
        snapshot = timestamp;
        value = v;
    }



  unsigned size() {
    return sizeof(uint64_t) + value.size();
  }
};

template <typename T>
class SnapshotIsolationLattice : public Lattice<SnapshotIsolationPayload<T>> {
 protected:
  void do_merge(const SnapshotIsolationPayload<T> &p) {
        // Current version is more recent
        if (this->element.snapshot > p.snapshot){
            this->element.snapshot = p.snapshot;
            this->element.value = p.value;
        }
  }

 public:
    SnapshotIsolationLattice() :
      Lattice<SnapshotIsolationPayload<T>>(SnapshotIsolationPayload<T>()) {}
    SnapshotIsolationLattice(const SnapshotIsolationPayload<T> &p) :
      Lattice<SnapshotIsolationPayload<T>>(p) {}
  MaxLattice<unsigned> size() { return {this->element.size()}; }
};

template <typename K, typename V>
class MapSILattice : public Lattice<std::map<K, V, std::greater<K>>> {
protected:
    void insert_pair(const K &k, const V &v) {
        auto search = this->element.find(k);
        if (search != this->element.end()) {
            static_cast<V *>(&(search->second))->merge(v);
        } else {
            // need to copy v since we will be "growing" it within the lattice
            V new_v = v;
            this->element.emplace(k, new_v);
        }
    }

    void do_merge(const std::map<K, V, std::greater<K>> &m) {
        for (const auto &pair : m) {
            this->insert_pair(pair.first, pair.second);
        }
    }

public:
    MapSILattice() : Lattice<std::map<K, V, std::greater<K>>>(std::map<K, V, std::greater<K>>()) {}
    MapSILattice(const std::map<K, V, std::greater<K>> &m) : Lattice<std::map<K, V, std::greater<K>>>(m) {}
    MaxLattice<unsigned> size() const { return this->element.size(); }

    MapSILattice<K, V> intersect(MapSILattice<K, V> other) const {
        MapSILattice<K, V> res;
        map<K, V> m = other.reveal();

        for (const auto &pair : m) {
            if (this->contains(pair.first).reveal()) {
                res.insert_pair(pair.first, this->at(pair.first));
                res.insert_pair(pair.first, pair.second);
            }
        }

        return res;
    }

    MapSILattice<K, V> project(bool (*f)(V)) const {
        map<K, V> res;
        for (const auto &pair : this->element) {
            if (f(pair.second)) res.emplace(pair.first, pair.second);
        }
        return MapSILattice<K, V>(res);
    }

    BoolLattice contains(K k) const {
        auto it = this->element.find(k);
        if (it == this->element.end())
            return BoolLattice(false);
        else
            return BoolLattice(true);
    }

    SetLattice<K> key_set() const {
        set<K> res;
        for (const auto &pair : this->element) {
            res.insert(pair.first);
        }
        return SetLattice<K>(res);
    }

    V &at(K k) { return this->element[k]; }

    bool has_upper_bound(K k) {
        auto it = this->element.upper_bound(k);
        return it != this->element.end();
    }

    V &upper_bound(K k) {
        auto it = this->element.upper_bound(k);
        return this->element.at(it->first);
    }

    void remove(K k) {
        auto it = this->element.find(k);
        if (it != this->element.end()) this->element.erase(k);
    }

    void insert(const K &k, const V &v) { this->insert_pair(k, v); }
};


#endif  // INCLUDE_LATTICES_MULTI_KEY_SNAPSHOT_ISOLATION_LATTICE_HPP
