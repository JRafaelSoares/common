//
// Created by jrsoares on 30/03/21.
//

#ifndef FAASSI_CONFLICT_MANAGER_CLIENT_H
#define FAASSI_CONFLICT_MANAGER_CLIENT_H

#endif //FAASSI_CONFLICT_MANAGER_CLIENT_H

#include <zmq.hpp>
#include "snapshot_isolation.pb.h"
class ConflictManagerClientInterface {
public:
    virtual zmq::context_t* get_context() = 0;
    virtual vector<SIResponse> receive_async() = 0;
    virtual void get_key_async(const Key& key, time_t snapshot) = 0;
    virtual void get_key_async(vector<Key> keys, vector<time_t> snapshot) = 0;
    virtual void get_key_version_async(const Key& key, time_t snapshot) = 0;
    virtual void get_key_version_async(vector<Key> keys, time_t snapshot) = 0;
    virtual void commit_async() = 0;
};