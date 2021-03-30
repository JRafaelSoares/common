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

#ifndef INCLUDE_THREADS_HPP_
#define INCLUDE_THREADS_HPP_

#include "types.hpp"

// The port on which clients send key address requests to routing nodes.
const unsigned kKeyAddressPort = 6450;

// The port on which clients receive responses from the KVS.
const unsigned kUserResponsePort = 6800;

// The port on which clients receive responses from the routing tier.
const unsigned kUserKeyAddressPort = 6850;

// The port on which cache nodes listen for updates from the KVS.
const unsigned kCacheUpdatePort = 7150;

// The port on which conflict manager nodes listen for commit prepare requests
const unsigned commitPreparePort = 7200;

// The port on which conflict manager nodes listen for commit  requests
const unsigned commitPort = 7250;

// The port on which coordinator conflict manager receives the request to initiates a commit
const unsigned commitBeginPort = 7300;

// The port on which conflict managers listen for key version requests
const unsigned keyVersionRequestPort = 7350;

// The port on which conflict managers listen for key requests
const unsigned keyRequestPort = 7400;

// The port on which coordinator conflict manager receives the request to initiates a commit
const unsigned clientKeyGetPort = 7450;

// The port on which conflict managers listen for key version requests
const unsigned clientKeyVersionGetPort = 7500;

// The port on which conflict managers listen for key requests
const unsigned clientCommitPort = 7550;

const string kBindBase = "tcp://*:";

class CacheThread {
  Address ip_;
  Address ip_base_;
  unsigned tid_;

 public:
  CacheThread(Address ip, unsigned tid) :
      ip_(ip),
      ip_base_("tcp://" + ip_ + ":"),
      tid_(tid) {}

  Address ip() const { return ip_; }

  unsigned tid() const { return tid_; }

  Address cache_get_bind_address() const { return "ipc:///requests/get"; }

  Address cache_get_connect_address() const { return "ipc:///requests/get"; }

  Address cache_put_bind_address() const { return "ipc:///requests/put"; }

  Address cache_put_connect_address() const { return "ipc:///requests/put"; }

  Address cache_update_bind_address() const {
    return kBindBase + std::to_string(tid_ + kCacheUpdatePort);
  }

  Address cache_update_connect_address() const {
    return ip_base_ + std::to_string(tid_ + kCacheUpdatePort);
  }
};

// For communication between conflict manager and cache
class CMCacheThread {
    Address ip_;
    Address ip_base_;
    unsigned tid_;

public:
    CMCacheThread(Address ip, unsigned tid) :
            ip_(ip),
            ip_base_("tcp://" + ip_ + ":"),
            tid_(tid) {}

    Address ip() const { return ip_; }

    unsigned tid() const { return tid_; }

    // Get requests to conflict manager cache
    Address cache_get_bind_address() const { return "ipc:///requests/cm_get"; }

    Address cache_get_connect_address() const { return "ipc:///requests/cm_get"; }

    // Put requests to conflict manager cache
    Address cache_put_bind_address() const { return "ipc:///requests/cm_put"; }

    Address cache_put_connect_address() const { return "ipc:///requests/cm_put"; }

    // Get requests response from conflict manager cache
    Address cache_get_response_bind_address() const { return "ipc:///requests/cm_get_response_" + std::to_string(tid()); }

    Address cache_get_response_connect_address() const { return "ipc:///requests/cm_get_response_" + std::to_string(tid()); }

    // Put requests response from conflict manager cache
    Address cache_put_response_bind_address() const { return "ipc:///requests/cm_put_response_" + std::to_string(tid()); }

    Address cache_put_response_connect_address() const { return "ipc:///requests/cm_put_response_" + std::to_string(tid()); }
};

// Communication between Conflict managers
class ConflictManagerThread {
    Address ip_;
    Address ip_base_;
    unsigned tid_;

public:
    ConflictManagerThread(Address ip, unsigned tid) :
            ip_(ip),
            ip_base_("tcp://" + ip_ + ":"),
            tid_(tid) {}

    Address ip() const { return ip_; }

    unsigned tid() const { return tid_; }

    // Commit prepare requests from coordinators
    Address commit_prepare_connect_address() const {
        return ip_base_ + std::to_string(tid_ + commitPreparePort);
    }

    Address commit_prepare_bind_address() const {
        return kBindBase + std::to_string(tid_ + commitPreparePort);
    }

    // Commit finish requests from coordinators

    Address commit_connect_address() const {
        return ip_base_ + std::to_string(tid_ + commitPort);
    }

    Address commit_bind_address() const {
        return kBindBase + std::to_string(tid_ + commitPort);
    }

    // Commit begin requests from coordinators
    Address commit_begin_connect_address() const {
        return ip_base_ + std::to_string(tid_ + commitBeginPort);
    }

    Address commit_begin_bind_address() const {
        return kBindBase + std::to_string(tid_ + commitBeginPort);
    }

    // Request key version
    Address key_version_request_connect_address() const {
        return ip_base_ + std::to_string(tid_ + keyVersionRequestPort);
    }

    Address key_version_request_bind_address() const {
        return kBindBase + std::to_string(tid_ + keyVersionRequestPort);
    }

    // Request key
    Address key_request_connect_address() const {
        return ip_base_ + std::to_string(tid_ + keyRequestPort);
    }

    Address key_request_bind_address() const {
        return kBindBase + std::to_string(tid_ + keyRequestPort);
    }
};

// ConflictManagerClient threads
class ConflictManagerClientThread {
    Address ip_;
    Address ip_base_;
    unsigned tid_;

public:
    ConflictManagerClientThread() {}
    ConflictManagerClientThread(Address ip, unsigned tid) :
            ip_(ip),
            tid_(tid),
            ip_base_("tcp://" + ip_ + ":") {}

    Address ip() const { return ip_; }

    unsigned tid() const { return tid_; }

    Address key_get_response_connect_address() const {
        return ip_base_ + std::to_string(tid_ + clientKeyGetPort);
    }

    Address key_get_response_bind_address() const {
        return kBindBase + std::to_string(tid_ + clientKeyGetPort);
    }

    Address key_get_version_response_connect_address() const {
        return ip_base_ + std::to_string(tid_ + clientKeyVersionGetPort);
    }

    Address key_get_version_response_bind_address() const {
        return kBindBase + std::to_string(tid_ + clientKeyVersionGetPort);
    }

    Address commit_response_connect_address() const {
        return ip_base_ + std::to_string(tid_ + clientCommitPort);
    }

    Address commit_response_bind_address() const {
        return kBindBase + std::to_string(tid_ + clientCommitPort);
    }
};


class UserRoutingThread {
  Address ip_;
  Address ip_base_;
  unsigned tid_;

 public:
  UserRoutingThread() {}

  UserRoutingThread(Address ip, unsigned tid) :
      ip_(ip),
      tid_(tid),
      ip_base_("tcp://" + ip_ + ":") {}

  Address ip() const { return ip_; }

  unsigned tid() const { return tid_; }

  Address key_address_connect_address() const {
    return ip_base_ + std::to_string(tid_ + kKeyAddressPort);
  }

  Address key_address_bind_address() const {
    return kBindBase + std::to_string(tid_ + kKeyAddressPort);
  }
};

class UserThread {
  Address ip_;
  Address ip_base_;
  unsigned tid_;

 public:
  UserThread() {}
  UserThread(Address ip, unsigned tid) :
      ip_(ip),
      tid_(tid),
      ip_base_("tcp://" + ip_ + ":") {}

  Address ip() const { return ip_; }

  unsigned tid() const { return tid_; }

  Address response_connect_address() const {
    return ip_base_ + std::to_string(tid_ + kUserResponsePort);
  }

  Address response_bind_address() const {
    return kBindBase + std::to_string(tid_ + kUserResponsePort);
  }

  Address key_address_connect_address() const {
    return ip_base_ + std::to_string(tid_ + kUserKeyAddressPort);
  }

  Address key_address_bind_address() const {
    return kBindBase + std::to_string(tid_ + kUserKeyAddressPort);
  }
};

#endif  // INCLUDE_THREADS_HPP_
