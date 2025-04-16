//
// Created by sanhuazhang on 2019/05/02
//

/*
 * Tencent is pleased to support the open source community by making
 * WCDB available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company.
 * All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *       https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "HandlePool.hpp"
#include "Assertion.hpp"
#include "CoreConst.h"
#include "FileManager.hpp"
#include "InnerHandle.hpp"
#include "Notifier.hpp"

namespace WCDB {

#pragma mark - Initialize
HandlePool::HandlePool(const UnsafeStringView &thePath) : path(thePath)
{
}

HandlePool::~HandlePool()
{
    //wait until all handles back.
    drain(nullptr);
}

#pragma mark - Concurrency
bool HandlePool::isNumberOfHandlesAllowed() const
{
    WCTAssert(m_concurrency.readSafety());
    WCTAssert(m_memory.readSafety());
    return numberOfAliveHandles() <= HandlePoolMaxAllowedNumberOfHandles;
}

void HandlePool::blockade()
{
    m_concurrency.lock();
}

void HandlePool::unblockade()
{
    WCTRemedialAssert(m_concurrency.writeSafety(), "Unblockade should not be called without blockaded.", return;);
    m_concurrency.unlock();
}

bool HandlePool::isBlockaded() const
{
    return m_concurrency.isLocked();
}

void HandlePool::didDrain()
{
}

void HandlePool::drain(const HandlePool::DrainedCallback &onDrained)
{
    WCTRemedialAssert(m_concurrency.level() != SharedLock::Level::Read,
                      "There are some threaded handles not invalidated.",
                      return;);
    LockGuard concurrencyGuard(m_concurrency);
    LockGuard memoryGuard(m_memory);
    clearAllHandles();
    didDrain();
    if (onDrained != nullptr) {
        onDrained();
        // double-clear since there might be some operations inside the drained block.
        clearAllHandles();
        didDrain();
    }
}

void HandlePool::clearAllHandles()
{
    WCTAssert(m_concurrency.writeSafety());
    WCTAssert(m_memory.writeSafety());
    for (unsigned int i = 0; i < HandleSlotCount; ++i) {
        m_frees[i].clear();
        auto &handles = m_handles[i];
        for (const auto &handle : handles) {
            handle->close();
        }
        handles.clear();
    }
}

#pragma mark - Handle
void HandlePool::purge()
{
    SharedLockGuard concurrencyGuard(m_concurrency);
    LockGuard memoryGuard(m_memory);
    for (unsigned int i = 0; i < HandleSlotCount; ++i) {
        auto &handles = m_handles[i];
        auto &frees = m_frees[i];
        for (const auto &handle : frees) {
            handle->close();
            handles.erase(handle);
        }
        frees.clear();
    }
}

size_t HandlePool::numberOfAliveHandles() const
{
    size_t count = 0;
    {
        SharedLockGuard concurrencyGuard(m_concurrency);
        SharedLockGuard memoryGuard(m_memory);
        for (const auto &handles : m_handles) {
            count += handles.size();
        }
    }
    return count;
}

bool HandlePool::isAliving() const
{
    bool aliving = false;
    {
        SharedLockGuard concurrencyGuard(m_concurrency);
        SharedLockGuard memoryGuard(m_memory);
        for (const auto &handles : m_handles) {
            if (handles.size() > 0) {
                aliving = true;
                break;
            }
        }
    }
    return aliving;
}

const std::set<std::shared_ptr<InnerHandle>> &HandlePool::getHandlesOfSlot(HandleSlot slot)
{
    WCTAssert(m_concurrency.readSafety());
    WCTAssert(m_memory.readSafety());
    WCTAssert(slot < HandleSlotCount);
    return m_handles[slot];
}

size_t HandlePool::numberOfAliveHandlesInSlot(const HandleSlot slot) const
{
    SharedLockGuard concurrencyGuard(m_concurrency);
    SharedLockGuard memoryGuard(m_memory);
    return m_handles[slot].size();
}

// 从句柄池中获取一个可复用的数据库连接句柄（InnerHandle），用于数据库操作。
// type：表示句柄的类型（用途）；writeHint：提示该连接可能用于写操作。
RecyclableHandle HandlePool::flowOut(HandleType type, const bool writeHint)
{
    // 根据连接类型获取对应的槽位（句柄分类）
    const HandleSlot slot = slotOfHandleType(type);
    WCTAssert(slot < HandleSlotCount);

    // 根据连接类型获取连接的类别（读、写、备份等）
    const HandleCategory category = categoryOfHandleType(type);
    WCTAssert(category < HandleCategoryCount);

    // 获取当前线程对应的线程本地句柄引用（若无则创建）
    ReferencedHandle &referencedHandle = m_threadedHandles.getOrCreate().at(category);

    // 检查当前线程是否已有缓存的连接句柄
    {
        // threaded handles is thread safe.
        if (referencedHandle.handle != nullptr) {
            WCTAssert(m_concurrency.readSafety());
            WCTAssert(referencedHandle.reference > 0);
            // 检查句柄当前是否由此线程使用
            WCTAssert(referencedHandle.handle->isUsingInThread(Thread::getCurrentThreadId()));
            // 增加引用计数（表示同一个连接被重复使用）
            ++referencedHandle.reference;
            // 返回一个可回收的句柄，设置回收函数为flowBack
            return RecyclableHandle(referencedHandle.handle,std::bind(&HandlePool::flowBack, this, type, std::placeholders::_1));
        }
    }

    // 若连接数量已达最大限制，则报错并返回nullptr
    if (!m_counter.tryIncreaseHandleCount(type, writeHint)) {
        Error error(Error::Code::Exceed,
                    Error::Level::Error,
                    "The operating count of database exceeds the maximum allowed.");
        error.infos.insert_or_assign("MaxAllowed", HandlePoolMaxAllowedNumberOfHandles);
        error.infos.insert_or_assign(ErrorStringKeyPath, path);
        Notifier::shared().notify(error);
        setThreadedError(std::move(error));
        return nullptr;
    }

    // 获取共享锁，确保并发安全（允许多个线程读）
    SharedLockGuard concurrencyGuard(m_concurrency);
    std::shared_ptr<InnerHandle> handle;

    {
        // 获取内存锁以访问m_frees（存放空闲连接句柄）
        LockGuard memoryGuard(m_memory);
        auto &freeSlot = m_frees[slot];
        // 如果存在空闲连接句柄，则复用该连接
        if (!freeSlot.empty()) {
            handle = freeSlot.back();
            WCTAssert(handle != nullptr);
            freeSlot.pop_back(); // 从空闲列表中移除
        }
    }

    // 若没有可复用连接句柄，则新建一个
    if (handle == nullptr) {
        // 创建新的数据库连接（底层调用 sqlite3_open_v2）
        handle = generateSlotedHandle(type);
        if (handle == nullptr) {
            // 若创建失败，减少连接计数并返回nullptr
            m_counter.decreaseHandleCount(writeHint);
            return nullptr;
        }

        // 创建成功后，放入句柄池的管理容器中
        LockGuard memoryGuard(m_memory);
        WCTAssert(m_handles[slot].find(handle) == m_handles[slot].end());
        m_handles[slot].emplace(handle);

        // 如果超过允许的连接数，则清理其他空闲句柄
        if (!isNumberOfHandlesAllowed()) {
            purge(); // 清理无用连接
            WCTAssert(isNumberOfHandlesAllowed());
        }
    } else {
        // 若句柄已复用，调用willReuseSlotedHandle检查连接是否健康
        if (!willReuseSlotedHandle(type, handle.get())) {
            // 若连接不适合复用，关闭句柄并移除
            handle->close();
            {
                LockGuard memoryGuard(m_memory);
                // 从管理容器中移除失败的连接句柄
                m_handles[slot].erase(handle);
            }
            m_counter.decreaseHandleCount(writeHint);
            return nullptr;
        }
    }

    // 此时连接句柄一定有效（非空）
    WCTAssert(handle != nullptr);

    // 设置连接的写入提示（writeHint）
    handle->setWriteHint(writeHint);
    // 标记该连接当前被哪个线程占用
    handle->setActiveThreadId(Thread::getCurrentThreadId());

    // 加锁，确保连接句柄引用赋值线程安全
    m_concurrency.lockShared();
    WCTAssert(referencedHandle.handle == nullptr && referencedHandle.reference == 0);
    // 缓存连接到线程本地，以便后续本线程复用
    referencedHandle.handle = handle;
    referencedHandle.reference = 1;

    // 返回一个封装了连接的 RecyclableHandle，并绑定回收函数 flowBack
    return RecyclableHandle(handle,std::bind(&HandlePool::flowBack, this, type, std::placeholders::_1));
}

void HandlePool::flowBack(HandleType type, const std::shared_ptr<InnerHandle> &handle)
{
    WCTAssert(handle != nullptr);
    WCTAssert(m_concurrency.readSafety());

    HandleSlot slot = slotOfHandleType(type);
    WCTAssert(slot < HandleSlotCount);
    HandleCategory category = categoryOfHandleType(type);
    WCTAssert(category < HandleCategoryCount);

    ReferencedHandle &referencedHandle = m_threadedHandles.getOrCreate().at(category);
    WCTAssert(referencedHandle.handle == handle);
    WCTAssert(referencedHandle.reference > 0);
    if (--referencedHandle.reference == 0) {
        handle->configTransactionEvent(nullptr);
        referencedHandle.handle = nullptr;
        bool writeHint = handle->getWriteHint();
        WCTRemedialAssert(
        !handle->isPrepared(), "Statement is not finalized.", handle->finalize(););
        handle->detachCancellationSignal();
        handle->finalizeStatements();
        {
            LockGuard memoryGuard(m_memory);
            m_frees[slot].push_back(handle);
            handle->setWriteHint(false);
            handle->setActiveThreadId(0);
        }
        m_concurrency.unlockShared();
        m_counter.decreaseHandleCount(writeHint);
    }
}

HandlePool::ReferencedHandle::ReferencedHandle() : handle(nullptr), reference(0)
{
}

} //namespace WCDB
