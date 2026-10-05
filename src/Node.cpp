#include"Node.h"
#include<system_error>
#include<unistd.h>
#include<fcntl.h>
#include<sys/epoll.h>
#include<sys/timerfd.h>
#include<sys/uio.h>
#include<sys/socket.h>
#include<cstring>
#include<arpa/inet.h>

class FdGuard
{
private:
    int fd_;
public:
    explicit FdGuard(int fd=-1):fd_(fd){}
    ~FdGuard(){if(fd_>=0)::close(fd_);}

    FdGuard(const FdGuard&)=delete;
    FdGuard& operator=(const FdGuard&)=delete;

    int get()const{return fd_;}
};

void set_nonblocking(int fd)
{
    int flags=fcntl(fd,F_GETFL,0);
    if(flags==-1)
    {
        throw std::system_error(errno,std::generic_category(),"fcntl F_GETFL");
    }
    if(fcntl(fd,F_SETFL,flags|O_NONBLOCK)==-1)
    {
        throw std::system_error(errno,std::generic_category(),"fcntl F_SETFL");
    }
}


Node::Node(NodeId my_id,uint16_t listen_port):my_id_(my_id),listen_port_(listen_port),router_(my_id),buffer_pool_(1024)
{
    sock_fd_=socket(AF_INET,SOCK_DGRAM,0);
    if(sock_fd_<0)
    {
        throw std::system_error(errno,std::generic_category(),"socket");
    }
    sock_guard_=std::make_unique<FdGuard>(sock_fd_);
    set_nonblocking(sock_fd_);

    int broadcast_enabled=1;
    if(setsockopt(sock_fd_,SOL_SOCKET,SO_BROADCAST,&broadcast_enabled,sizeof(broadcast_enabled))<0)
    {
        throw std::system_error(errno,std::generic_category(),"setsockopt SO_BROADCAST");
    }

    sockaddr_in local_addr{};
    local_addr.sin_family=AF_INET;
    local_addr.sin_addr.s_addr=INADDR_ANY;
    local_addr.sin_port=htons(listen_port_);
    if(bind(sock_fd_,reinterpret_cast<sockaddr*>(&local_addr),sizeof(local_addr))<0)
    {
        throw std::system_error(errno,std::generic_category(),"bind");
    }

    //静态路由（示例拓扑）
    if(my_id_==1)
    {
        router_.add_static_route(3,make_addr("127.0.0.1",8889));
    }
    else if(my_id_==2)
    {
        router_.add_static_route(1,make_addr("127.0.0.1",8888));
        router_.add_static_route(3,make_addr("127.0.0.1",8890));
    }
    else if(my_id_==3)
    {
        router_.add_static_route(1,make_addr("127.0.0.1",8889));
    }

    epoll_fd_=epoll_create1(0);
    if(epoll_fd_<0)
    {
        throw std::system_error(errno,std::generic_category(),"wpoll_create1");
    }
    epoll_guard_=std::make_unique<FdGuard>(epoll_fd_);

    epoll_event ev{};
    ev.events=EPOLLIN;
    ev.data.fd=sock_fd_;
    if(epoll_ctl(epoll_fd_,EPOLL_CTL_ADD,sock_fd_,&ev)<0)
    {
        throw std::system_error(errno,std::generic_category(),"epoll_ctl add sock");
    }

    timer_fd_=timerfd_create(CLOCK_MONOTONIC,TFD_NONBLOCK);
    if(timer_fd_<0)
    {
        throw std::system_error(errno,std::generic_category(),"timer_create");
    }
    timer_guard_=std::make_unique<FdGuard>(timer_fd_);

    struct itimerspec ts{};
    ts.it_interval.tv_nsec=100'000'000;
    ts.it_value.tv_nsec=100'000'000;

    if(timerfd_settime(timer_fd_,0,&ts,nullptr)<0)
    {
        throw std::system_error(errno,std::generic_category(),"timefd_settime");
    }

    ev.events=EPOLLIN;
    ev.data.fd=timer_fd_;
    if(epoll_ctl(epoll_fd_,EPOLL_CTL_ADD,timer_fd_,&ev)<0)
    {
        throw std::system_error(errno,std::generic_category(),"epoll_ctl add timer");
    }

    log(LogLevel::INFO,"Node "+std::to_string(my_id_)+" strated on port "+std::to_string(listen_port_));
}

void Node::run()
{
    const int MAX_EVENTS=16;
    epoll_event events[MAX_EVENTS];

    while(true)
    {
        int nfds=epoll_wait(epoll_fd_,events,MAX_EVENTS,-1);

        if(nfds<0)
        {
            if(errno==EINTR)
            {
                continue;
            }
            throw std::system_error(errno,std::generic_category(),"epoll_wait");
        }

        for(int i=0;i<nfds;++i)
        {
            int fd=events[i].data.fd;
            if(fd==sock_fd_)
            {
                handle_udp_recv();
            }
            else
            {
                handle_timer();
            }
        }
    }
}

void Node::handle_udp_recv()
{
    char recv_buf[4096];
    sockaddr_in from_addr{};
    socklen_t from_len=sizeof(from_addr);
    ssize_t n=recvfrom(sock_fd_,recv_buf,sizeof(recv_buf),0,reinterpret_cast<sockaddr*>(&from_addr),&from_len);
    if(n<0)
    {
        if(errno==EAGAIN||errno==EWOULDBLOCK)
        {
            return;
        }
        throw std::system_error(errno,std::generic_category(),"recvfrom");
    }

    if(n<static_cast<ssize_t>(HEADER_SIZE+8))
    {
        log(LogLevel::WARN,"Packet too short");
        return;
    }

    uint32_t seq,ack;
    uint16_t flags,length,checksum;
    deserialize_header(recv_buf,seq,ack,flags,length,checksum);

    if(length>MAX_DATA_LEN||n<static_cast<ssize_t>(HEADER_SIZE+8+length))
    {
        log(LogLevel::WARN,"Invalid or truncated packet length");
        return;
    }

    uint16_t computed=compute_packet_checksum(recv_buf,HEADER_SIZE,recv_buf+HEADER_SIZE+8,length);
    if(computed!=checksum)
    {
        log(LogLevel::ERROR,"Checksum mismatch, drop packet");
        return;
    }

    NodeId src_id,dst_id;
    std::memcpy(&src_id,recv_buf+HEADER_SIZE,4);
    std::memcpy(&dst_id,recv_buf+HEADER_SIZE+4,4);
    src_id=ntohl(src_id);
    dst_id=ntohl(dst_id);

    log(LogLevel::INFO, "Recv from"+add_to_string(from_addr)+
                        " seq="+std::to_string(seq)+
                        " flags="+std::to_string(flags)+
                        " src="+std::to_string(src_id)+
                        " dst="+std::to_string(dst_id));
    
    if(flags&FLAG_HELLO)
    {
        router_.handle_hello(src_id,from_addr);
        return;
    }

    if(flags&FLAG_RREQ)
    {
        if(length<sizeof(uint32_t))
        {
            log(LogLevel::WARN,"RREQ packet is missing its request ID");
            return;
        }
        uint32_t rreq_id;//简化：从payload提取（假设前4字节）
        std::memcpy(&rreq_id,recv_buf+HEADER_SIZE+8,4);
        rreq_id=ntohl(rreq_id);
        router_.handle_rreq(recv_buf,n,src_id,dst_id,rreq_id,from_addr,sock_fd_);
        return;
    }

    if(flags&FLAG_RREP)
    {
        if(length<sizeof(NodeId))
        {
            log(LogLevel::WARN,"RREP packet is missing its advertised destination");
            return;
        }
        //处理RREP：更新路由表到目标（从payload提取目标节点ID）
        NodeId target;
        std::memcpy(&target,recv_buf+HEADER_SIZE+8,4);
        target=ntohl(target);
        router_.add_static_route(target,from_addr);
        log(LogLevel::INFO,"Added route to "+std::to_string(target)+" via "+add_to_string(from_addr));
        if(dst_id!=my_id_)
        {
            auto next_hop=router_.lookup(dst_id);
            if(!next_hop)
            {
                log(LogLevel::WARN,"No reverse route for RREP destination "+std::to_string(dst_id));
                return;
            }
            if(sendto(sock_fd_,recv_buf,n,0,reinterpret_cast<const sockaddr*>(&(*next_hop)),sizeof(*next_hop))<0)
            {
                log(LogLevel::ERROR,"Failed to forward RREP: "+std::string(strerror(errno)));
            }
        }
        return;
    }

    if(dst_id==my_id_)
    {
        auto it=connections_.find(src_id);
        if(it==connections_.end())
        {
            connections_.emplace(src_id,Connection(src_id,from_addr,my_id_));
            it=connections_.find(src_id);
        }
        auto next_hop_opt=router_.lookup(src_id);
        const sockaddr_in& next_hop=next_hop_opt?*next_hop_opt:from_addr;
        it->second.handle_packet(recv_buf,n,src_id,dst_id,seq,ack,flags,length,sock_fd_,next_hop);
    }
    else
    {
        auto next_hop=router_.lookup(dst_id);
        if(!next_hop)
        {
            log(LogLevel::WARN,"No route to "+std::to_string(dst_id));
            //可发起RREQ（此处略）
            return;
        }
        ssize_t sent=sendto(sock_fd_,recv_buf,n,0,reinterpret_cast<sockaddr*>(&(*next_hop)),sizeof(*next_hop));
        if(sent>0)
        {
            log(LogLevel::INFO,"Forwarded to "+add_to_string(*next_hop));
        }
    }
}


void Node::handle_timer()
{
    uint64_t exp;
    ssize_t s=read(timer_fd_,&exp,sizeof(exp));
    if(s!=sizeof(exp))
    {
        return;
    }

    static int hello_counter=0;
    if(++hello_counter%5==0)
    {
        send_hello();
    }

    for(auto& [id,conn]:connections_)
    {
        auto next_hop=router_.lookup(id);
        if(next_hop)
        {
            conn.check_timeout(sock_fd_,*next_hop);
        }
    }

    //节点1 主动发送数据
    if(my_id_==1)
    {
        static int msg_id=0;
        if(connections_.find(3)==connections_.end())
        {
            auto addr=make_addr("127.0.0.1",8890);
            connections_.emplace(3,Connection(3,addr,my_id_));
        }
        auto& conn=connections_[3];
        if(conn.state()==Connection::State::CLOSED||conn.state()==Connection::State::ESTABLISHED)
        {
            //自动转为ESTABLISHED
            //直接发送
        }
        auto next_hop=router_.lookup(3);
        if(next_hop)
        {
            std::string msg="Hello from A #"+std::to_string(msg_id++);
            conn.send_data(msg,buffer_pool_,sock_fd_,*next_hop);
        }
    }
}

void Node::send_hello()
{
    char packet[HEADER_SIZE+8]{};
    char* header=packet;
    serialize_header(0,0,FLAG_HELLO,0,0,header);
    uint16_t csum=compute_packet_checksum(header,HEADER_SIZE,nullptr,0);
    serialize_header(0,0,FLAG_HELLO,0,csum,header);
    uint32_t net_src=htonl(my_id_);
    uint32_t net_dst=htonl(0);
    std::memcpy(packet+HEADER_SIZE,&net_src,sizeof(net_src));
    std::memcpy(packet+HEADER_SIZE+4,&net_dst,sizeof(net_dst));

    sockaddr_in broadcast{};
    broadcast.sin_family=AF_INET;
    broadcast.sin_addr.s_addr=htonl(INADDR_BROADCAST);
    broadcast.sin_port=htons(DEFAULT_PORT);
    if(sendto(sock_fd_,packet,sizeof(packet),0,reinterpret_cast<sockaddr*>(&broadcast),sizeof(broadcast))<0)
    {
        log(LogLevel::ERROR,"Failed to send HELLO: "+std::string(strerror(errno)));
    }
}

sockaddr_in Node::make_addr(const std::string& ip,uint16_t port)
{
    sockaddr_in addr{};
    addr.sin_family=AF_INET;
    addr.sin_port=htons(port);
    inet_pton(AF_INET,ip.c_str(),&addr.sin_addr);
    return addr;
}
