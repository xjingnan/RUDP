#pragma once

#include"rudp_common.h"
#include"BufferPool.h"
#include<unordered_map>
#include<memory>
#include<deque>
#include<chrono>
#include<optional>
#include<string_view>

class Connection
{
public:
    enum class State{CLOSED,SYN_SENT,ESTABLISHED,FIN_WAIT,CLOSE_WAIT};
private:
    struct SendPacket
    {
        uint32_t seq;
        std::vector<char> data;
        std::chrono::steady_clock::time_point send_time;
        uint8_t retrans_count=0;
        bool acked=false;
        size_t len=0;
    };

private:
    void handle_ack(uint32_t ack_seq,int sock_fd,const sockaddr_in& next_hop);
    void handle_data(const char* buffer,size_t len,uint32_t seq,uint16_t length);
    void deliver_data(std::string_view data);
    void update_rtt(std::chrono::milliseconds rtt_samples);

    
private:
    /* data */
    NodeId remote_id_;
    NodeId my_id_;
    sockaddr_in remote_addr_;
    State state_;
    uint32_t next_seq_;
    uint32_t base_seq_;
    uint32_t expected_seq_;
    uint32_t cwnd_;
    uint32_t ssthresh_;
    std::chrono::milliseconds rtt_;      // 平滑后的 RTT (SRTT)
    std::chrono::milliseconds rtt_var_;  // RTT 方差 (RTTVAR)
    std::chrono::milliseconds rto_;      // 超时重传时间 (RTO)
    std::unordered_map<uint32_t,std::shared_ptr<SendPacket>> send_packets_;
    std::unordered_map<uint32_t, std::string> recv_buffer_;
    uint32_t dup_ack_count_ = 0; // 记录连续收到相同 ACK 的次数
public:
    Connection(NodeId remote_id,sockaddr_in remote_addr,NodeId my_id);
    ~Connection();

public:
    void send_data(const std::string& data,BufferPool& pool,int sock_fd,const sockaddr_in& next_hop);
    void handle_packet(const char* buffer,size_t len,NodeId src_id,NodeId dst_id,uint32_t seq,uint32_t ack,uint16_t flags,uint16_t length,int sock_fd, const sockaddr_in& next_hop);
    void check_timeout(int sock_fd,const sockaddr_in& next_hop);
    State state()const {return state_;}
};