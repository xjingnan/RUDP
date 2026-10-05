#include"Router.h"
#include<sys/uio.h>
#include<unistd.h>
#include<cstring>

Router::Router(NodeId my_id):my_id_(my_id){}

void Router::add_static_route(NodeId dst,sockaddr_in next_hop)
{
    routing_table_[dst]=next_hop;
}

std::optional<sockaddr_in> Router::lookup(NodeId dst)
{
    auto it=routing_table_.find(dst);
    if(it!=routing_table_.end())
    {
        return it->second;
    }

    return std::nullopt;
}

void Router::handle_hello(NodeId src,const sockaddr_in& src_addr)
{
    neighbors_[src]=src_addr;
    log(LogLevel::INFO,"Neighbor discovered: "+std::to_string(src));
}

void Router::handle_rreq(const char* buffer,size_t len,NodeId src,NodeId dst,uint32_t rreq_id,const sockaddr_in& src_addr,int sock_fd)
{
    if(rreq_seen_.find(rreq_id)!=rreq_seen_.end())
    {
        return;
    }
    rreq_seen_.insert(rreq_id);

    //学习反向路由
    routing_table_[src]=src_addr;

    if(dst==my_id_||routing_table_.find(dst)!=routing_table_.end())
    {
        //自己是目标或者已有路由，回复RREP
        send_rrep(src,src_addr,dst,sock_fd);
    }
    else
    {
        //广播转发RREQ
        for(const auto& [nbr,addr]:neighbors_)
        {
            if(addr.sin_addr.s_addr==src_addr.sin_addr.s_addr&&addr.sin_port==src_addr.sin_port)
            {
                continue;
            }
            ssize_t sent=sendto(sock_fd,buffer,len,0,reinterpret_cast<const sockaddr*>(&addr),sizeof(addr));
            if(sent>0)
            {
                log(LogLevel::INFO,"Forwarded RREQ to neighbor "+std::to_string(nbr));
            }
        }
    }
}

void Router::send_rrep(NodeId dst,const sockaddr_in& dst_addr,NodeId advertised_dst,int sock_fd)
{
    char packet[HEADER_SIZE+12]{};
    char* header=packet;
    const uint16_t payload_len=sizeof(NodeId);
    serialize_header(0,0,FLAG_RREP,payload_len,0,header);
    NodeId net_src=htonl(my_id_);
    NodeId net_dst=htonl(dst);
    NodeId net_advertised_dst=htonl(advertised_dst);
    std::memcpy(packet+HEADER_SIZE,&net_src,sizeof(net_src));
    std::memcpy(packet+HEADER_SIZE+4,&net_dst,sizeof(net_dst));
    std::memcpy(packet+HEADER_SIZE+8,&net_advertised_dst,sizeof(net_advertised_dst));
    uint16_t csum=compute_packet_checksum(header,HEADER_SIZE,packet+HEADER_SIZE+8,payload_len);
    serialize_header(0,0,FLAG_RREP,payload_len,csum,header);

    if(sendto(sock_fd,packet,sizeof(packet),0,reinterpret_cast<const sockaddr*>(&dst_addr),sizeof(dst_addr))<0)
    {
        log(LogLevel::ERROR,"sendto RREP failed");
    }
    else
    {
        log(LogLevel::INFO,"Sent RREP to "+std::to_string(dst));
    }
}

