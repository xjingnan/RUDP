#include"Connection.h"

#include<sys/uio.h>
#include<unistd.h>
#include<cstring>
#include<iostream>

Connection::Connection(NodeId remote_id,sockaddr_in remote_addr,NodeId my_id)
    :remote_id_(remote_id),my_id_(my_id),remote_addr_(remote_addr),state_(State::CLOSED),next_seq_(1),base_seq_(1),expected_seq_(1),cwnd_(1),ssthresh_(64),rtt_(std::chrono::milliseconds(0)),rtt_var_(std::chrono::milliseconds(0)),rto_(std::chrono::milliseconds(200)){}


void Connection::send_data(const std::string& data,BufferPool& pool,int sock_fd,const sockaddr_in& next_hop)
{
    if(state_!=State::ESTABLISHED)
    {
        //简化：自动进入ESTABLISHED状态
        state_=State::ESTABLISHED;
    }
    if(send_packets_.size()>=SEND_WINDOW_SIZE)
    {
        log(LogLevel::WARN,"Send window full, drop data");
        return;
    }

    uint32_t seq=next_seq_++;
    uint16_t data_len=static_cast<uint16_t>(data.size());

    //构造头部
    char header[HEADER_SIZE];
    serialize_header(seq,0,FLAG_DATA,data_len,0,header);
    uint16_t checksum=compute_packet_checksum(header,HEADER_SIZE,data.data(),data_len);
    serialize_header(seq,0,FLAG_DATA,data_len,checksum,header);

    NodeId net_src=htonl(my_id_);
    NodeId net_dst=htonl(remote_id_);

    //使用iover零拷贝发送
    struct iovec iov[4];
    iov[0].iov_base=header;
    iov[0].iov_len=HEADER_SIZE;
    iov[1].iov_base=&net_src;
    iov[1].iov_len=4;
    iov[2].iov_base=&net_dst;
    iov[2].iov_len=4;
    iov[3].iov_base=const_cast<char*>(data.data());
    iov[3].iov_len=data_len;

    msghdr msg{};
    msg.msg_iov=iov;
    msg.msg_iovlen=4;
    msg.msg_name=const_cast<sockaddr*>(reinterpret_cast<const sockaddr*>(&next_hop));
    msg.msg_namelen=sizeof(next_hop);

    ssize_t sent=sendmsg(sock_fd,&msg,0);
    if(sent<0)
    {
        log(LogLevel::ERROR,"sendmsg failed: "+std::string(strerror(errno)));
        return;
    }

    //保存完整包用于重传
    std::vector<char> packet(HEADER_SIZE+8+data_len);
    std::memcpy(packet.data(),header,HEADER_SIZE);
    std::memcpy(packet.data()+HEADER_SIZE,&net_src,4);
    std::memcpy(packet.data()+HEADER_SIZE+4,&net_dst,4);
    std::memcpy(packet.data()+HEADER_SIZE+8,data.data(),data_len);

    auto pkt=std::make_shared<SendPacket>();
    pkt->seq=seq;
    pkt->data=std::move(packet);
    pkt->send_time=std::chrono::steady_clock::now();
    pkt->retrans_count=0;
    pkt->acked=false;
    pkt->len=HEADER_SIZE+8+data_len;
    send_packets_[seq]=pkt;

    log(LogLevel::INFO,"Send seq="+std::to_string(seq)+" len="+std::to_string(data_len));
}

void Connection::handle_packet(const char* buffer,size_t len,NodeId src_id,NodeId dst_id,uint32_t seq,uint32_t ack,uint16_t flags,uint16_t length,int sock_fd, const sockaddr_in& next_hop)
{
    if(flags&FLAG_ACK)
    {
        handle_ack(ack, sock_fd, next_hop);
    }
    if(flags&FLAG_DATA)
    {
        handle_data(buffer,len,seq,length);
    }
}

void Connection::handle_ack(uint32_t ack_seq,int sock_fd,const sockaddr_in& next_hop)
{
    if(ack_seq>=base_seq_)
    {
        auto it=send_packets_.find(ack_seq);
        if(it!=send_packets_.end()&&!it->second->acked)
        {
            // 收到新确认，清零重复 ACK 计数
            dup_ack_count_ = 0;

            it->second->acked=true;
            auto now=std::chrono::steady_clock::now();
            auto rtt_sample=std::chrono::duration_cast<std::chrono::milliseconds>(now-it->second->send_time);
            update_rtt(rtt_sample);

            //拥塞控制
            if(cwnd_<ssthresh_)
            {
                cwnd_+=1;//慢启动
            }
            else
            {
                cwnd_+=1.0/cwnd_;//拥塞避免
            }

            while(send_packets_.find(base_seq_)!=send_packets_.end()&&send_packets_[base_seq_]->acked)
            {
                send_packets_.erase(base_seq_);
                base_seq_++;
            }
        }
    }
    else
    {
        // 2. 收到了“重复 ACK”（ack_seq < base_seq_，说明 base_seq_ 丢了）
        dup_ack_count_++;

        if(dup_ack_count_==3)
        {
            //温和惩罚，网络轻微拥塞
            ssthresh_=std::max(cwnd_/2,1u);
            cwnd_=ssthresh_;

            log(LogLevel::WARN, "Fast Retransmit! cwnd halved to " + std::to_string(cwnd_));

            // 快速重传丢失的包（base_seq_ 就是最早未确认的包）
            auto it=send_packets_.find(base_seq_);
            if(it!=send_packets_.end())
            {
               ssize_t sent=sendto(sock_fd,it->second->data.data(),it->second->len,0,reinterpret_cast<const sockaddr*>(&next_hop),sizeof(next_hop));
                if (sent > 0) {
                    it->second->send_time = std::chrono::steady_clock::now();
                    it->second->retrans_count++;
                }
            }
            // 重置计数器，防止重复触发
            dup_ack_count_ = 0; 
        }
    }
}

void Connection::handle_data(const char* buffer,size_t len,uint32_t seq,uint16_t length)
{
    if(length==0)return;
    const char* payload=buffer+HEADER_SIZE+8;
    std::string_view data_view(payload,length);
    if(seq==expected_seq_)
    {
        deliver_data(data_view);
        expected_seq_++;;
        while(recv_buffer_.find(expected_seq_)!=recv_buffer_.end())
        {
            deliver_data(recv_buffer_[expected_seq_]);
            recv_buffer_.erase(expected_seq_);
            expected_seq_++;
        }
    }
    else if (seq>expected_seq_&&seq-expected_seq_<RECV_WINDOW_SIZE)
    {
        if(recv_buffer_.find(seq)==recv_buffer_.end())
        {
            recv_buffer_[seq]==std::string(data_view);
            log(LogLevel::INFO,"Cached seq="+std::to_string(seq));
        }
    }
    else
    {
        log(LogLevel::WARN,"Duplicate or out-of-order seq="+std::to_string(seq));
    }
}

void Connection::deliver_data(std::string_view data)
{
    std::string msg(data);
    log(LogLevel::INFO,"Delivered: "+msg);
}

void Connection::check_timeout(int sock_fd,const sockaddr_in& next_hop)
{
    auto now=std::chrono::steady_clock::now();
    for(auto& pair:send_packets_)
    {
        auto& pkt=pair.second;
        if(pkt->acked)
        {
            continue;
        }
        auto elapsed=std::chrono::duration_cast<std::chrono::milliseconds>(now-pkt->send_time);
        if(elapsed>=rto_)
        {
            ssthresh_=std::max(cwnd_/2,1u);
            cwnd_=1;
            if(pkt->retrans_count>=MAX_RETRANSMIT)
            {
                log(LogLevel::ERROR,"Max restrainsmit for seq "+std::to_string(pkt->seq));

            }
            else
            {
                size_t sent=sendto(sock_fd,pkt->data.data(),pkt->len,0,reinterpret_cast<const sockaddr*>(&next_hop),sizeof(next_hop));

                if(sent>0)
                {
                    pkt->send_time=now;
                    pkt->retrans_count++;
                    rto_ = rto_ = std::min(rto_ * 2, std::chrono::milliseconds(60000));;
                    log(LogLevel::INFO,"Retransmit seq "+std::to_string(pkt->seq)+" count="+std::to_string(pkt->retrans_count));
                }
            }
            break;
        }
    }
}

void Connection::update_rtt(std::chrono::milliseconds rtt_samples)
{
    // 1. 如果是第一次计算 RTT（初始化）
    if (rtt_.count() == 0) {
        rtt_ = rtt_samples;
        rtt_var_ = rtt_samples / 2; // 初始方差设为样本的一半
        rto_ = rtt_ + std::max(rtt_var_ * 4, std::chrono::milliseconds(1));
        return;
    }

    // 2. 更新 RTT 方差 (RTTVAR)
    // 公式: RTTVAR = (1 - β) * RTTVAR + β * |SRTT - RTT_sample|
    // 标准 TCP 中 β = 1/4 = 0.25
    auto diff = std::abs(rtt_.count() - rtt_samples.count());
    rtt_var_ = std::chrono::milliseconds(
        static_cast<uint64_t>(0.75 * rtt_var_.count() + 0.25 * diff)
    );

    // 3. 更新平滑 RTT (SRTT)
    // 公式: SRTT = (1 - α) * SRTT + α * RTT_sample
    // 标准 TCP 中 α = 1/8 = 0.125
    rtt_ = std::chrono::milliseconds(
        static_cast<uint64_t>(0.875 * rtt_.count() + 0.125 * rtt_samples.count())
    );

    // 4. 计算 RTO
    // 公式: RTO = SRTT + max(G, 4 * RTTVAR)
    // G 是时钟粒度，这里我们取 1 毫秒作为最小粒度
    rto_ = rtt_ + std::max(rtt_var_ * 4, std::chrono::milliseconds(1));
}


