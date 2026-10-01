#pragma once

#include<cstdint>
#include<arpa/inet.h>
#include<string>
#include<chrono>
#include<vector>
#include<cstring>
#include<iostream>

//常量
constexpr uint16_t DEFAULT_PORT=8888;
constexpr size_t MAX_DATA_LEN=1024-8;//预留源/目标ID
constexpr size_t MAX_RETRANSMIT=5;
constexpr size_t SEND_WINDOW_SIZE=64;
constexpr size_t RECV_WINDOW_SIZE=64;
constexpr size_t HEADER_SIZE=14; //seq(4)+ack(4)+flags(2)+length(2)+checksum(2)

using NodeId=uint32_t;

//标志位
constexpr uint16_t FLAG_ACK=0x0001;
constexpr uint16_t FLAG_SYN=0x0002;
constexpr uint16_t FLAG_FIN=0x0004;
constexpr uint16_t FLAG_DATA=0x0008;
constexpr uint16_t FLAG_HELLO=0x0010;
constexpr uint16_t FLAG_RREQ=0x0020;
constexpr uint16_t FLAG_RREP=0x0040;

//辅助函数
inline std::string add_to_string(const sockaddr_in& addr)
{
    char buf[INET_ADDRSTRLEN];
    inet_ntop(AF_INET,&addr.sin_addr,buf,sizeof(buf));
    return std::string(buf)+":"+std::to_string(ntohs(addr.sin_port));
}

//CRC16校验和（CCITT）
inline uint16_t crc16(const uint8_t* data,size_t len)
{
    uint16_t crc=0xFFFF;
    for(size_t i=0;i<len;++i)
    {
        crc^=data[i]<<8;
        for(int j=0;j<8;++j)
        {
            if(crc&0x8000)
            {
                crc=(crc<<1)^0x1021;
            }
            else
            {
                crc<<=1;
            }
        }
    }
    return crc;
}

//序列化
inline void serialize_header(uint32_t seq,uint32_t ack,uint16_t flags,uint16_t length,uint16_t checksum,char* buf)
{
    uint32_t n_seq=htonl(seq);
    uint32_t n_ack=htonl(ack);
    uint16_t n_flags=htons(flags);
    uint16_t n_len=htons(length);
    uint16_t n_csum=htons(checksum);
    std::memcpy(buf,&n_seq,4);
    std::memcpy(buf+4,&n_ack,4);
    std::memcpy(buf+8,&n_flags,2);
    std::memcpy(buf+10,&n_len,2);
    std::memcpy(buf+12,&n_csum,2);
}

//反序列化
inline void deserialize_header(const char*buf,uint32_t& seq,uint32_t& ack,uint16_t& flags,uint16_t& length,uint16_t& checksum)
{
    uint32_t n_seq,n_ack;
    uint16_t n_flags,n_len,n_csum;
    std::memcpy(&n_seq,buf,4);
    std::memcpy(&n_ack,buf+4,4);
    std::memcpy(&n_flags,buf+8,2);
    std::memcpy(&n_len,buf+10,2);
    std::memcpy(&n_csum,buf+12,2);
    seq=ntohl(n_seq);
    ack=ntohl(n_ack);
    flags=ntohs(n_flags);
    length=ntohs(n_len);
    checksum=ntohs(n_csum);
}

//计算整个包的校验和
inline uint16_t compute_packet_checksum(const char* header,size_t header_len,const char* data,size_t data_len)
{
    std::vector<uint8_t> combined(header_len+data_len);
    std::memcpy(combined.data(),header,header_len);
    std::memcpy(combined.data()+header_len,data,data_len);
    combined[12]=0;
    combined[13]=0;
    return crc16(combined.data(),combined.size());
}

//日志工具
enum class LogLevel{INFO,WARN,ERROR};

inline void log(LogLevel level,const std::string& msg)
{
    const char* level_str[]={"INFO","WARN","ERROR"};
    auto now=std::chrono::system_clock::now();
    auto t=std::chrono::system_clock::to_time_t(now);
    std::cout<<"["<<::ctime(&t)<<"]"<<level_str[static_cast<int>(level)]<<" "<<msg<<std::endl;
}