#include "arduino_bridge/arduino_bridge.hpp"

namespace arduino_bridge {

ArduinoBridge::ArduinoBridge() : sock_fd_(-1), msg_counter_(0), running_(false) {}

ArduinoBridge::~ArduinoBridge() {
  disconnect();
}

bool ArduinoBridge::connect() {
  sock_fd_ = socket(AF_UNIX, SOCK_STREAM, 0);
  if (sock_fd_ < 0) {
    std::cerr << "Failed to create socket: " << strerror(errno) << std::endl;
    return false;
  }

  struct sockaddr_un addr;
  memset(&addr, 0, sizeof(addr));
  addr.sun_family = AF_UNIX;
  strncpy(addr.sun_path, "/var/run/arduino-router.sock", sizeof(addr.sun_path) - 1);

  if (::connect(sock_fd_, reinterpret_cast<struct sockaddr *>(&addr), sizeof(addr)) < 0) {
    std::cerr << "Failed to connect: " << strerror(errno) << std::endl;
    close(sock_fd_);
    sock_fd_ = -1;
    return false;
  }

  running_ = true;
  recv_thread_ = std::thread(&ArduinoBridge::receive_loop, this);

  return true;
}

void ArduinoBridge::disconnect() {
  running_ = false;
  if (sock_fd_ >= 0) {
      shutdown(sock_fd_, SHUT_RDWR);
    }
  if (recv_thread_.joinable()) {
      recv_thread_.join();
    }
  if (sock_fd_ >= 0) {
    close(sock_fd_);
    sock_fd_ = -1;
  }
}

void ArduinoBridge::receive_loop() {
  char buffer[4096];
  msgpack::unpacker unpacker;

  while (running_) {
    ssize_t received = recv(sock_fd_, buffer, sizeof(buffer), 0);
    if (received <= 0) break;

    unpacker.reserve_buffer(received);
    memcpy(unpacker.buffer(), buffer, received);
    unpacker.buffer_consumed(received);

    msgpack::object_handle oh;
    while (unpacker.next(oh)) {
        handle_response(oh.get());
    }
  }
}

void ArduinoBridge::handle_response(const msgpack::object & obj) {
  if (obj.type != msgpack::type::ARRAY) return;

  auto arr = obj.via.array;
  if (arr.size < 4) return;

  int type = arr.ptr[0].as<int>();
  if (type != 1) return;

  uint32_t msgid = arr.ptr[1].as<uint32_t>();

  std::lock_guard<std::mutex> lock(response_mutex_);
  auto it = pending_responses_.find(msgid);
  if (it != pending_responses_.end()) {
    if (!arr.ptr[2].is_nil()) {
      it->second.error = arr.ptr[2].as<std::string>();
    } else {
      it->second.success = true;
      it->second.result = msgpack::clone(arr.ptr[3]);
    }
    response_cv_.notify_all();
  }
}

}  // namespace arduino_bridge