#ifndef ARDUINO_BRIDGE__ARDUINO_BRIDGE_HPP_
#define ARDUINO_BRIDGE__ARDUINO_BRIDGE_HPP_

#include <msgpack.hpp>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>
#include <vector>
#include <string>
#include <sstream>
#include <iostream>
#include <map>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <cstring>
#include <chrono>
#include <utility>

namespace arduino_bridge {

class ArduinoBridge {
public:
    struct Response {
        bool success;
        msgpack::object_handle result;
        std::string error;
    };

private:
    int sock_fd_;
    uint32_t msg_counter_;
    std::thread recv_thread_;
    bool running_;
    std::mutex response_mutex_;
    std::condition_variable response_cv_;
    std::map<uint32_t, Response> pending_responses_;

public:
    ArduinoBridge();
    ~ArduinoBridge();

    bool connect();
    void disconnect();

    template<typename... Args>
    Response call(const std::string & method, Args... args) {
        int type = 0;
        uint32_t msgid = ++msg_counter_;

        std::vector<msgpack::type::variant> params;
        pack_args(params, args...);

        std::stringstream buffer;
        msgpack::pack(buffer, std::make_tuple(type, msgid, method, params));
        std::string data = buffer.str();

        {
            std::lock_guard<std::mutex> lock(response_mutex_);
            pending_responses_.emplace(msgid, Response{false, msgpack::object_handle(), ""});
        }

        ssize_t sent = send(sock_fd_, data.c_str(), data.size(), 0);
        if (sent != static_cast<ssize_t>(data.size())) {
            std::lock_guard<std::mutex> lock(response_mutex_);
            pending_responses_.erase(msgid);
            return Response{false, msgpack::object_handle(), "Send failed"};
        }

        std::unique_lock<std::mutex> lock(response_mutex_);
        bool received = response_cv_.wait_for(
            lock,
            std::chrono::seconds(5),
            [this, msgid]() {
                return pending_responses_[msgid].success || !pending_responses_[msgid].error.empty();
            }
        );

        if (!received) {
            pending_responses_.erase(msgid);
            return Response{false, msgpack::object_handle(), "Timeout"};
        }

        Response response = std::move(pending_responses_.at(msgid));
        pending_responses_.erase(msgid);

        return response;
    }

    template<typename... Args>
    bool notify(const std::string & method, Args... args) {
        int type = 2;

        std::vector<msgpack::type::variant> params;
        pack_args(params, args...);

        std::stringstream buffer;
        msgpack::pack(buffer, std::make_tuple(type, method, params));
        std::string data = buffer.str();

        ssize_t sent = send(sock_fd_, data.c_str(), data.size(), 0);
        return sent == static_cast<ssize_t>(data.size());
    }

private:
    void receive_loop();
    void handle_response(const msgpack::object & obj);

    template<typename T, typename... Rest>
    void pack_args(std::vector<msgpack::type::variant> & params, T first, Rest... rest) {
        params.push_back(msgpack::type::variant(first));
        pack_args(params, rest...);
    }

    void pack_args(std::vector<msgpack::type::variant> & /*params*/) {}
};

}  // namespace arduino_bridge

#endif  // ARDUINO_BRIDGE__ARDUINO_BRIDGE_HPP_