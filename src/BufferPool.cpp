#include "BufferPool.h"
#include <stdexcept>

BufferPool::BufferPool(size_t num_blocks) {
    // 预分配所有内存块，所有权牢牢绑定在 memory_pool_ 中
    memory_pool_.reserve(num_blocks);
    for (size_t i = 0; i < num_blocks; ++i) {
        auto block = std::make_unique<char[]>(BLOCK_SIZE);
        // 将裸指针放入空闲队列
        free_queue_.push(block.get());
        // 将所有权移入池中
        memory_pool_.push_back(std::move(block));
    }
}

std::pair<char*, size_t> BufferPool::allocate() {
    // 如果空闲队列空了，动态扩容（向系统申请新内存）
    if (free_queue_.empty()) {
        auto block = std::make_unique<char[]>(BLOCK_SIZE);
        char* ptr = block.get();
        free_queue_.push(ptr);
        memory_pool_.push_back(std::move(block));
    }

    // 从空闲队列头部取出一个指针返回
    char* ptr = free_queue_.front();
    free_queue_.pop();
    
    return {ptr, BLOCK_SIZE};
}

void BufferPool::deallocate(char* ptr) {
    // 基础防御：防止传入空指针
    if (ptr == nullptr) return;

    // 生产环境优化：可以在此处加一个断言或检查，
    // 确保 ptr 确实属于这个内存池（防止外部乱传野指针导致池子污染）
    
    // 将指针重新放回空闲队列，等待下一次分配
    free_queue_.push(ptr);
}