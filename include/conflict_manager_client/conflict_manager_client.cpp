//
// Created by jrsoares on 30/03/21.
//

#include "conflict_manager_client.h"

using TimePoint = std::chrono::time_point<std::chrono::system_clock>;

struct PendingRequests {
    PendingRequests() = default;
    PendingRequests(set<Key> read_set, TimePoint tp, KeyRequest request) :
        read_set_(read_set),
        tp_(tp),
        request_(request){
        response_.set_type(request.type());
    }

    KeyRequest request_;
    set<Key> read_set_;
    KeyResponse response_;
    TimePoint tp_;
};

struct PendingCommitRequests {
    PendingCommitRequests() = default;
    PendingCommitRequests(set<Key> read_set, TimePoint tp, CommitRequest request) :
            read_set_(read_set),
            tp_(tp),
            request_(request){
        response_.set_request_id(request.request_id());
    }

    CommitRequest request_;
    set<Key> read_set_;
    CommitResponse response_;
    TimePoint tp_;
};

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

    vector<KeyResponse> receive_async() {
        vector<KeyResponse> result;
        kZmqUtil->poll(0, &pollitems_);
        if (pollitems_[0].revents & ZMQ_POLLIN) {
            string serialized = kZmqUtil->recv_string(&key_get_response_puller_);
            KeyResponse response;
            response.ParseFromString(serialized);

            if (pending_requests_.find(response.response_id()) != pending_requests_.end()){
                PendingRequests pending = pending_requests_[response.response_id()];

                for (const auto &tuple : response.tuples()) {
                    Key key = tuple.key();
                    pending.read_set_.erase(key);
                    auto tup = pending.response_.add_tuples();
                    tup->set_key(key);
                    tup->set_lattice_type(tuple.lattice_type());
                    tup->set_payload(tuple.payload());
                    tup->set_error(tuple.error());
                }

                if (pending.read_set_.empty()){
                    result.push_back(pending.response_);
                    pending_requests_.erase(response.response_id());
                }
            } else {
                log_->error("Request does not exist");
            }
        }

        if (pollitems_[1].revents & ZMQ_POLLIN) {
            string serialized = kZmqUtil->recv_string(&key_get_version_response_puller_);
            KeyResponse response;
            response.ParseFromString(serialized);

            if (pending_requests_.find(response.response_id()) != pending_requests_.end()){
                auto &pending = pending_requests_[response.response_id()];

                for (const auto &tuple : response.tuples()) {
                    Key key = tuple.key();
                    pending.read_set_.erase(key);
                    auto tup = pending.response_.add_tuples();
                    tup->set_key(key);
                    tup->set_payload(tuple.payload());
                    tup->set_error(tuple.error());
                }

                if (pending.read_set_.empty()){
                    result.push_back(pending.response_);
                    pending_requests_.erase(response.response_id());
                }
            } else {
                log_->error("Request does not exist");
            }
        }

        /*
        // GC the pending request map
        set<string> to_remove;
        for (const auto& pair : pending_requests_) {
            if (std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now() - pair.second.tp_)
                        .count() > timeout_) {
                // query to the routing tier timed out
                result.push_back(generate_bad_response(pair.second.request_));

                to_remove.insert(pair.first);
            }
        }

        for (const auto& request : to_remove) {
            pending_requests_.erase(request);
        }
         */
        return result;
    }

    vector<CommitResponse> receive_commit_async(){
        vector<CommitResponse> result;
        kZmqUtil->poll(0, &pollitems_);
        if (pollitems_[2].revents & ZMQ_POLLIN) {
            string serialized = kZmqUtil->recv_string(&commit_response_puller_);
            CommitResponse response;
            response.ParseFromString(serialized);

            if (pending_commit_requests_.find(response.request_id()) != pending_commit_requests_.end()){

                auto &pending = pending_commit_requests_[response.request_id()];
                if (response.abort_flag() != CommitError::C_NO_ERROR){
                    pending.response_.set_abort_flag(response.abort_flag());
                    result.push_back(pending.response_);
                }
                for (const auto &key : response.committed_keys()) {
                    pending.read_set_.erase(key);
                    pending.response_.set_commit_time(response.commit_time());
                }

                if (pending.read_set_.empty()){
                    result.push_back(pending.response_);
                    pending_commit_requests_.erase(response.request_id());
                }
            } else {
                log_->error("Request does not exist");
            }
        }

        // GC the pending request map
        /*
        set<string> to_remove;
        for (const auto& pair : pending_commit_requests_) {
            if (std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now() - pair.second.tp_)
                        .count() > timeout_) {
                // query to the routing tier timed out
                pending_commit_requests_[pair.first].response_.set_abort_flag(CommitError::C_TIMEOUT);
                result.push_back(pending_commit_requests_[pair.first].response_);

                to_remove.insert(pair.first);
            }
        }
        for (const auto& request : to_remove) {
            pending_commit_requests_.erase(request);
        }
         */
        return result;
    }
    zmq::context_t* get_context() { return &context_; }

    void get_key_async(const Key& key, time_t snapshot){
        // transform key into a vector
        set<Key> keys_requested;
        keys_requested.insert(key);
        get_key_async(keys_requested, snapshot);
    }

    void get_key_async(set<Key> keys, time_t snapshot){
        KeyRequest request;
        request.set_type(RequestType::GET);
        request.set_response_address(cmct_.key_get_response_connect_address());
        string request_id = get_request_id(snapshot);
        request.set_request_id(request_id);
        request.set_snapshot(snapshot);

        for (auto const& key: keys){
            KeyTuple* tuple = request.add_tuples();
            tuple->set_key(key);
        }
        pending_requests_.emplace(request_id, PendingRequests(keys, std::chrono::system_clock::now(), request));
        Address worker = get_key_worker_thread();
        send_request<KeyRequest>(request, socket_cache_[worker]);
    }

    void get_key_version_async(const Key& key, time_t snapshot){
        set<Key> keys_requested;
        keys_requested.insert(key);
        get_key_version_async(keys_requested, snapshot);
    }

    void get_key_version_async(set<Key> keys, time_t snapshot){
        KeyRequest request;
        request.set_type(RequestType::GET_VERSION);
        request.set_response_address(cmct_.key_get_version_response_connect_address());
        string request_id = get_request_id(snapshot);
        request.set_request_id(request_id);
        request.set_snapshot(snapshot);

        for (auto const& key: keys){
            KeyTuple* tuple = request.add_tuples();
            tuple->set_key(key);
        }
        pending_requests_.emplace(request_id, PendingRequests(keys, std::chrono::system_clock::now(), request));
        Address worker = get_key_version_worker_thread();
        send_request<KeyRequest>(request, socket_cache_[worker]);

    }

    void commit_async(vector<Key> keys, vector<string> payloads, LatticeType type, time_t snapshot){
        // Make "PUT" request for the keys to be committed
        KeyRequest request;
        request.set_type(RequestType::PUT);
        request.set_snapshot(snapshot);
        string request_id = get_request_id(snapshot);
        request.set_request_id(request_id);
        set<Key> key_set;
        for (int key = 0; key < keys.size(); key++){
            auto tuple = request.add_tuples();
            tuple->set_key(keys[key]);
            tuple->set_lattice_type(type);
            tuple->set_payload(payloads[key]);
            key_set.insert(keys[key]);
        }
        string serialized_key_request;
        request.SerializeToString(&serialized_key_request);

        // Make commit request
        CommitRequest commit_request;
        Address worker = commit_worker_thread();

        commit_request.set_commit_type(CommitType::C_BEGIN);
        commit_request.set_coordinator_address(worker);
        commit_request.set_request_id(request_id);
        commit_request.set_key_request(serialized_key_request);
        Address response_address = cmct_.commit_response_connect_address();
        commit_request.set_client_address(response_address);
        pending_commit_requests_.emplace(request_id, PendingCommitRequests(key_set, std::chrono::system_clock::now(), commit_request));

        send_request<CommitRequest>(commit_request, socket_cache_[worker]);
    }

    // Get request id to correspond between
    string get_request_id(time_t snapshot) {
        return std::to_string(snapshot)+ "_" + cmct_.ip() + ":" + std::to_string(cmct_.tid());
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

    // Get which thread to send the commit request
    string commit_worker_thread(){
        //return conflict_manager_threads_[rand_r(&seed_) % conflict_manager_threads_.size()].key_request_connect_address();
        return conflict_manager_threads_[0].commit_connect_address();
    }

    KeyResponse generate_bad_response(const KeyRequest& req) {
        KeyResponse resp;

        resp.set_type(req.type());
        resp.set_response_id(req.request_id());
        resp.set_error(AnnaError::TIMEOUT);
        resp.set_snapshot(req.snapshot());

        for (auto const& tup: req.tuples()){
            KeyTuple* tp = resp.add_tuples();
            tp->set_key(tup.key());

            if (req.type() == RequestType::PUT) {
                tp->set_lattice_type(tup.lattice_type());
                tp->set_payload(tup.payload());
            }
        }

        return resp;
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

    map<string, PendingRequests> pending_requests_;
    map<string, PendingCommitRequests> pending_commit_requests_;

};