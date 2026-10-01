#pragma once

#include"rudp_common.h"
#include"BufferPool.h"
#include"Connection.h"
#include"Router.h"
#include<memory>
#include<unordered_map>
#include<arpa/inet.h>

class FdGuard;

class Node
{
private:
    NodeId my_id_;
    uint16_t listen_port_;
    int sock_fd_;
    int epoll_fd_;
    int timer_fd_;
    std::unique_ptr<FdGuard>sock_guard_;
    std::unique_ptr<FdGuard>epoll_guard_;
    std::unique_ptr<FdGuard>timer_guard_;
    Router router_;
    BufferPool buffer_pool_;
    std::unordered_map<NodeId,Connection>connections_;

private:
    void handle_udp_recv();
    void handle_timer();
    void send_hello();
    static sockaddr_in make_addr(const std::string& ip,uint16_t port);

public:
    Node(NodeId my_id,uint16_t listen_port);
    ~Node()=default;

    void run();
};


