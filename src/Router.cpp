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
        send_rrep(src,src_addr,sock_fd);
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

void Router::send_rrep(NodeId dst,const sockaddr_in& dst_addr,int sock_fd)
{
    char header[HEADER_SIZE];
    serialize_header(0,0,FLAG_RREP,0,0,header);
    uint16_t csum=compute_packet_checksum(header,HEADER_SIZE,nullptr,0);
    serialize_header(0,0,FLAG_RREP,0,csum,header);

    NodeId net_my=htonl(my_id_);
    struct iovec iov[2];
    iov[0].iov_base=header;
    iov[0].iov_len=HEADER_SIZE;
    iov[1].iov_base=&net_my;
    iov[1].iov_len=4;
    
    msghdr msg{};
    msg.msg_iov=iov;
    msg.msg_iovlen=2;
    msg.msg_name=const_cast<sockaddr*>(reinterpret_cast<const sockaddr*>(&dst_addr));
    msg.msg_namelen=sizeof(dst_addr);

    if(sendmsg(sock_fd,&msg,0)<0)
    {
        log(LogLevel::ERROR,"sendmsg RREP failed");
    }
    else
    {
        log(LogLevel::INFO,"Sent RREP to "+std::to_string(dst));
    }
}



