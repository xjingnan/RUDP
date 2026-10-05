#pragma once

#include"rudp_common.h"
#include<map>
#include<unordered_set>
#include<optional>


class Router
{
private:
    NodeId my_id_;
    std::map<NodeId,sockaddr_in> routing_table_;
    std::map<NodeId,sockaddr_in> neighbors_;
    std::unordered_set<uint32_t> rreq_seen_;
private:
    void send_rrep(NodeId dst,const sockaddr_in& dst_addr,NodeId advertised_dst,int sock_fd);
public:
    explicit Router(NodeId my_id);

    void add_static_route(NodeId dst,sockaddr_in next_hop);

    std::optional<sockaddr_in> lookup(NodeId dst);

    void handle_hello(NodeId src,const sockaddr_in& src_addr);

    void handle_rreq(const char* buffer,size_t len,NodeId src,NodeId dst,uint32_t rreq_id,const sockaddr_in& src_addr,int sock_fd);

};


