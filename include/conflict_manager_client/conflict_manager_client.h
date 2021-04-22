//
// Created by jrsoares on 30/03/21.
//

#ifndef FAASSI_CONFLICT_MANAGER_CLIENT_H
#define FAASSI_CONFLICT_MANAGER_CLIENT_H


#include "anna.pb.h"
#include "common.hpp"
#include "requests.hpp"
#include "threads.hpp"
#include "types.hpp"
#include "snapshot_isolation.pb.h"
class ConflictManagerClientInterface {
public:
    virtual zmq::context_t* get_context() = 0;
    virtual vector<KeyResponse> receive_async() = 0;
    virtual void get_key_async(const Key& key, time_t snapshot) = 0;
    virtual void get_key_async(set<Key> keys, time_t snapshot) = 0;
    virtual void get_key_version_async(const Key& key, time_t snapshot) = 0;
    virtual void get_key_version_async(set<Key> keys, time_t snapshot) = 0;
    virtual void commit_async(vector<Key> keys, vector<string> payloads, LatticeType type, time_t snapshot) = 0;
    virtual vector<CommitResponse> receive_commit_async() = 0;
};

#endif //FAASSI_CONFLICT_MANAGER_CLIENT_H
