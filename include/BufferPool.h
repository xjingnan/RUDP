#pragma once

#include<cstddef>
#include<memory>
#include<list>
#include<utility>
#include<vector>
#include<queue>


class BufferPool
{
private:
    /* data */
    // 1. 内存所有权池：永久持有所有内存块，直到 BufferPool 被销毁
    std::vector<std::unique_ptr<char[]>> memory_pool_;
    
    // 2. 空闲指针队列：只存裸指针，用于快速分发和回收
    std::queue<char*> free_queue_;
public:
    static constexpr size_t BLOCK_SIZE=2048;
    explicit BufferPool(size_t num_blocks=1024);

    // 禁止拷贝和赋值，防止内存被多次释放
    BufferPool(const BufferPool&) = delete;
    BufferPool& operator=(const BufferPool&) = delete;

    //分配一块内存，返回指针和大小
    std::pair<char*,size_t> allocate();

    //释放内存（实际上是放回空闲列表）
    void deallocate(char* ptr);

};
