//
// Created by jrsoares on 30/03/21.
//

#include "conflict_manager_client.h"
#include <google/protobuf/util/time_util.h>

class ConflictManagerClient : public ConflictManagerClientInterface {
public:
    ConflictManagerClient(vector<ConflictManagerThread> conflict_manager_threads,
                          string ip, unsigned tid = 0, unsigned timeout = 10000) :
      context_(zmq::context_t(1)),
      conflict_manager_threads_(conflict_manager_threads),
      cmct_(ConflictManagerClientThread(ip, tid)),
      socket_cache_(SocketCache(&context_, ZMQ_PUSH)),
      key_get_response_puller_(zmq::socket_t(context_, ZMQ_PULL)),
      key_get_version_response_puller_(zmq::socket_t(context_, ZMQ_PULL)),
      commit_response_puller_(zmq::socket_t(context_, ZMQ_PULL)),
      log_(spdlog::basic_logger_mt("cm_client_log", "cm_client_log.txt", true)),
      timeout_(timeout)
    {
        log_->flush_on(spdlog::level::info);

        std::hash<string> hasher;
        seed_ = time(NULL);
        seed_ += hasher(ip);
        seed_ += tid;
        log_->info("Random seed is {}.", seed_);

        // Bind the listening sockets
        key_get_response_puller_.bind(cmct_.key_get_response_bind_address());
        key_get_version_response_puller_.bind(cmct_.key_get_version_response_bind_address());
        commit_response_puller_.bind(cmct_.commit_response_bind_address());

        pollitems_ = {
                {static_cast<void*>(key_get_response_puller_), 0, ZMQ_POLLIN, 0},
                {static_cast<void*>(key_get_version_response_puller_), 0, ZMQ_POLLIN, 0},
                {static_cast<void*>(commit_response_puller_), 0, ZMQ_POLLIN, 0},
        };

        // set the request ID to 0
        rid_ = 0;
    }

    ~ConflictManagerClient() {}

public:

    vector<SIResponse> receive_async() {
        vector<SIResponse> result;
        kZmqUtil->poll(0, &pollitems_);
        if (pollitems_[0].revents & ZMQ_POLLIN) {

        }

        if (pollitems_[1].revents & ZMQ_POLLIN) {

        }

        if (pollitems_[2].revents & ZMQ_POLLIN) {

        }

        return result;
    }
    zmq::context_t* get_context() { return &context_; }

    void get_key_async(const Key& key, google::protobuf::Timestamp snapshot){
        // transform key into a vector
        vector<Key> keys_requested;
        keys_requested.push_back(key);
        vector<google::protobuf::Timestamp> timestamp_vector;
        timestamp_vector.push_back(snapshot);
        get_key_async(keys_requested, timestamp_vector);
    }

    void get_key_async(vector<Key> keys, vector<google::protobuf::Timestamp> snapshot){
        SIRequest request;
        request.set_response_address(cmct_.key_get_response_connect_address());
        request.set_request_id(get_request_id());

        for(Key key : keys){
            SITuple* tuple = request.add_tuples();
            tuple->set_key(key);
        }
        Address worker = get_key_worker_thread();
        send_request<SIRequest>(request, socket_cache_[worker]);
    }

    void get_key_version_async(const Key& key, google::protobuf::Timestamp snapshot){

    }

    void get_key_version_async(vector<Key> key, google::protobuf::Timestamp snapshot){

    }

    void commit_async(){

    }

    // Get request id to correspond between
    string get_request_id() {
        if (++rid_ % 10000 == 0) rid_ = 0;
        return cmct_.ip() + ":" + std::to_string(cmct_.tid()) + "_" +
               std::to_string(rid_++);
    }

    // Get which thread to send the get key request
    string get_key_worker_thread(){
        //return conflict_manager_threads_[rand_r(&seed_) % conflict_manager_threads_.size()].key_request_connect_address();
        return conflict_manager_threads_[0].key_request_connect_address();
    }

    // Get which thread to send the get key version request
    string get_key_version_worker_thread(){
        //return conflict_manager_threads_[rand_r(&seed_) % conflict_manager_threads_.size()].key_request_connect_address();
        return conflict_manager_threads_[0].key_version_request_connect_address();
    }


private:
    // the ZMQ context we use to create sockets
    zmq::context_t context_;

    // the IP and port functions for this thread
    ConflictManagerClientThread cmct_;

    vector<ConflictManagerThread> conflict_manager_threads_;

    // ZMQ receiving sockets
    zmq::socket_t key_get_response_puller_;
    zmq::socket_t key_get_version_response_puller_;
    zmq::socket_t commit_response_puller_;

    vector<zmq::pollitem_t> pollitems_;

    // the random seed for this client
    unsigned seed_;

    // class logger
    logger log_;

    // GC timeout
    unsigned timeout_;

    // the current request id
    unsigned rid_;

    // cache for opened sockets
    SocketCache socket_cache_;
};