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

#include "InnerDatabase.hpp"
#include "Assertion.hpp"
#include "FileManager.hpp"
#include "Notifier.hpp"
#include "Path.hpp"
#include "RepairKit.h"
#include "StringView.hpp"
#include "WCDBError.hpp"

#include "AssembleHandleOperator.hpp"
#include "BackupHandleOperator.hpp"
#include "CompressHandleOperator.hpp"
#include "IntegerityHandleOperator.hpp"
#include "MigrateHandleOperator.hpp"
#include "VacuumHandleOperator.hpp"

#include "CompressingHandleDecorator.hpp"
#include "MigratingHandleDecorator.hpp"

#include "AutoVacuumConfig.hpp"
#include "BasicConfig.hpp"
#include "BusyRetryConfig.hpp"
#include "CipherHandle.hpp"
#include "CommonCore.hpp"
#include "DBOperationNotifier.hpp"
#include "DecorativeHandle.hpp"
#include "SQLite.h"

#include <ctime>

namespace WCDB {

#pragma mark - Initializer
InnerDatabase::InnerDatabase(const UnsafeStringView &path)
: HandlePool(path)
, m_initialized(false)
, m_closing(0)
, m_tag(Tag::invalid())
, m_fullSQLTrace(false)
, m_liteModeEnable(false)
, m_factory(path)
, m_needLoadIncremetalMaterial(false)
, m_migration(this)
, m_migratedCallback(nullptr)
, m_compression(this)
, m_compressedCallback(nullptr)
, m_isInMemory(false)
, m_sharedInMemoryHandle(nullptr)
, m_mergeLogic(this)
{
    StringViewMap<Value> info;
    DBOperationNotifier::shared().notifyOperation(this, DBOperationNotifier::Operation::Create, info);
}

InnerDatabase::~InnerDatabase() = default;

#pragma mark - Basic
void InnerDatabase::setTag(const Tag &tag)
{
    LockGuard memoryGuard(m_memory);
    m_tag = tag;
    StringViewMap<Value> info;
    DBOperationNotifier::shared().notifyOperation(
    this, DBOperationNotifier::Operation::SetTag, info);
}

Tag InnerDatabase::getTag() const
{
    SharedLockGuard memoryGuard(m_memory);
    return m_tag;
}

bool InnerDatabase::canOpen()
{
    CommonCore::shared().skipIntegrityCheck(getPath());
    auto handle = getHandle();
    CommonCore::shared().skipIntegrityCheck(nullptr);
    return handle != nullptr;
}

void InnerDatabase::didDrain()
{
    WCTAssert(m_concurrency.writeSafety());
    WCTAssert(m_memory.writeSafety());
    WCTAssert(!isOpened());
    m_initialized = false;
}

bool InnerDatabase::checkShouldInterruptWhenClosing(const UnsafeStringView &sourceType)
{
    if (m_closing != 0) {
        Error error(Error::Code::Interrupt, Error::Level::Ignore, "Interrupt due to it's closing.");
        error.infos.insert_or_assign(ErrorStringKeyPath, path);
        error.infos.insert_or_assign(ErrorStringKeyType, sourceType);
        Notifier::shared().notify(error);
        setThreadedError(std::move(error));
        return true;
    }
    return false;
}

void InnerDatabase::close(const ClosedCallback &onClosed)
{
    if (m_isInMemory) {
        if (m_sharedInMemoryHandle != nullptr) {
            m_sharedInMemoryHandle->close();
        }
        m_sharedInMemoryHandle = nullptr;
        if (onClosed != nullptr) {
            onClosed();
            didDrain();
        }
        return;
    }
    ++m_closing;
    {
        SharedLockGuard concurrencyGuard(m_concurrency);
        SharedLockGuard memoryGuard(m_memory);
        // suspend auto checkpoint/backup/integrity check/migrate/compress/merge fts5 index
        for (auto &handle : getHandlesOfSlot(HandleSlot::HandleSlotAutoTask)) {
            handle->suspend(true);
        }
    }
    CommonCore::shared().stopAllDatabaseEvent(getPath());
    drain(onClosed);
    --m_closing;
}

bool InnerDatabase::isOpened() const
{
    return isAliving();
}

InnerDatabase::InitializedGuard InnerDatabase::initialize()
{
    do {
        {
            // Step 1: 获取共享锁，确保多线程访问安全，防止其他线程同时修改状态
            SharedLockGuard concurrencyGuard(m_concurrency);
            SharedLockGuard memoryGuard(m_memory);
            // 如果数据库已经初始化，则直接返回共享锁
            if (m_initialized) {
                return concurrencyGuard;
            }
        }

        // Step 2: 获取独占锁，确保初始化过程不会被其他线程干扰
        LockGuard concurrencyGuard(m_concurrency);
        LockGuard memoryGuard(m_memory);
        // 再次检查是否已经初始化（可能在等待锁期间其他线程已完成初始化）
        if (m_initialized) {
            // 如果已初始化，则重新进入循环进行检查
            continue;
        }
        // 如果是内存数据库，直接标记为已初始化并跳过后续步骤
        if (m_isInMemory) {
            m_initialized = true;
            continue;
        }
        // 设置错误路径，便于跟踪初始化过程中可能出现的错误
        CommonCore::shared().setThreadedErrorPath(path);
        // 确保数据库目录存在，不存在则尝试创建
        if (!FileManager::createDirectoryWithIntermediateDirectories(Path::getDirectory(path))) {
            // 记录错误信息并退出初始化
            assignWithSharedThreadedError();
            break;
        }
        // Step 3: 执行 Vacuum 操作，优化数据库结构，提高性能
        {
            Repair::FactoryVacuum vacuumer = m_factory.vacuumer();
            if (!vacuumer.work()) {
                // 如果 Vacuum 失败，记录错误信息并退出初始化
                setThreadedError(vacuumer.getError());
                break;
            }
        }
        // Step 4: 执行 Renewer 操作，可能用于恢复或重建数据库
        {
            Repair::FactoryRenewer renewer = m_factory.renewer();
            if (!renewer.work()) {
                // 如果 Renewer 失败，记录错误信息并退出初始化
                setThreadedError(renewer.getError());
                break;
            }
        }
        // Step 5: 检查数据库文件是否存在
        auto exists = FileManager::fileExists(path);
        if (!exists.succeed()) {
            // 如果检查失败，记录错误信息并退出初始化
            assignWithSharedThreadedError();
            break;
        }
        // 如果数据库文件不存在，则尝试创建一个空文件，以确保文件标识符稳定
        if (!exists.value() && !FileManager::createFile(path)) {
            assignWithSharedThreadedError();
            break;
        }
        // Step 6: 清空错误路径，标记初始化成功
        CommonCore::shared().setThreadedErrorPath(nullptr);
        m_initialized = true;
    } while (true);
    // 如果初始化失败，则返回 nullptr
    return nullptr;
}

#pragma mark - Config
void InnerDatabase::setConfigs(const Configs &configs)
{
    LockGuard memoryGuard(m_memory);
    m_configs = configs;
}

void InnerDatabase::setConfig(const UnsafeStringView &name,
                              const std::shared_ptr<Config> &config,
                              const int priority)
{
    LockGuard memoryGuard(m_memory);
    m_configs.insert(StringView(name), config, priority);
}

void InnerDatabase::removeConfig(const UnsafeStringView &name)
{
    LockGuard memoryGuard(m_memory);
    m_configs.erase(StringView(name));
}

void InnerDatabase::setFullSQLTraceEnable(bool enable)
{
    m_fullSQLTrace = enable;
}

void InnerDatabase::setLiteModeEnable(const bool enable)
{
    if (m_liteModeEnable != enable) {
        close([&] {
            m_liteModeEnable = enable;
            CommonCore::shared().enableAutoCheckpoint(this, !m_liteModeEnable);
        });
    }
}

bool InnerDatabase::liteModeEnable() const
{
    return m_liteModeEnable;
}

#pragma mark - Handle
RecyclableHandle InnerDatabase::getHandle(const bool writeHint)
{
    constexpr auto type = HandleType::Normal;
    if (m_isInMemory) {
        InitializedGuard initializedGuard = initialize();
        if (m_sharedInMemoryHandle == nullptr) {
            m_sharedInMemoryHandle = generateSlotedHandle(type);
        }
        return RecyclableHandle(m_sharedInMemoryHandle, nullptr);
    }
    // Additional shared lock is not needed because the threadedHandles is always empty when it's blocked. So threaded handles is thread safe.
    auto handle = m_transactionedHandles.getOrCreate();
    if (handle.get() != nullptr) {
        handle->configTransactionEvent(this);
        WCTAssert(m_concurrency.readSafety());
        return handle;
    }
    const InitializedGuard initializedGuard = initialize();
    if (!initializedGuard.valid()) {
        return nullptr;
    }
    handle = flowOut(type, writeHint);
    if (handle != nullptr) {
        handle->configTransactionEvent(this);
    }
    return handle;
}

bool InnerDatabase::execute(const Statement &statement)
{
    const RecyclableHandle handle = getHandle(statement.isWriteStatement());
    if (handle != nullptr) {
        if (handle->execute(statement)) {
            return true;
        }
        setThreadedError(handle->getError());
    }
    return false;
}

bool InnerDatabase::execute(const UnsafeStringView &sql)
{
    const RecyclableHandle handle = getHandle();
    if (handle != nullptr) {
        if (handle->execute(sql)) {
            return true;
        }
        setThreadedError(handle->getError());
    }
    return false;
}

Optional<bool> InnerDatabase::tableExists(const UnsafeStringView &table)
{
    Optional<bool> exists;
    RecyclableHandle handle = getHandle();
    if (handle != nullptr) {
        exists = handle->tableExists(table);
        if (!exists.succeed()) {
            setThreadedError(handle->getError());
        }
    }
    return exists;
}

StringView InnerDatabase::getRunningSQLInThread(uint64_t tid) const
{
    SharedLockGuard concurrencyGuard(m_concurrency);
    SharedLockGuard memoryGuard(m_memory);
    for (const auto &handles : m_handles) {
        for (const auto &handle : handles) {
            if (handle->isUsingInThread(tid)) {
                StringView sql = handle->getCurrentSQL();
                if (!sql.empty()) {
                    return sql;
                }
            }
        }
    }
    return StringView();
}

std::shared_ptr<InnerHandle> InnerDatabase::generateSlotedHandle(const HandleType type)
{
    WCTAssert(m_concurrency.readSafety());
    HandleSlot slot = slotOfHandleType(type);
    std::shared_ptr<InnerHandle> handle;
    switch (slot) {
    case HandleSlotNormal:
    case HandleSlotAutoTask:
        handle = std::make_shared<DecorativeHandle>();
        break;
    case HandleSlotCipher:
        handle = std::make_shared<CipherHandle>();
        break;
    default:
        WCTAssert(slot == HandleSlotAssemble || slot == HandleSlotVacuum);
        handle = std::make_shared<ConfiguredHandle>();
        break;
    }

    if (handle == nullptr) {
        setThreadedError(Error(Error::Code::NoMemory, Error::Level::Error));
        return nullptr;
    }

    handle->setThreadedErrorProne(this);

    if (!setupHandle(type, handle.get())) {
        return nullptr;
    }
    return handle;
}

bool InnerDatabase::willReuseSlotedHandle(HandleType type, InnerHandle *handle)
{
    return setupHandle(type, handle);
}

bool InnerDatabase::setupHandle(const HandleType type, InnerHandle *handle)
{
    WCTAssert(handle != nullptr);

    handle->setTag(getTag());
    handle->setType(type);
    handle->setLiteModeEnable(m_liteModeEnable);
    handle->setFullSQLTraceEnable(m_fullSQLTrace);
    handle->setBusyTraceEnable(CommonCore::shared().isBusyTraceEnable());
    const HandleSlot slot = slotOfHandleType(type);
    handle->enableWriteMainDB(m_liteModeEnable || slot == HandleSlotAutoTask || slot == HandleSlotAssemble || slot == HandleSlotVacuum);
    handle->markAsCanBeSuspended(false);
    handle->markErrorAsUnignorable(99); //Clear all ignorable code

    // Decoration
    if (slot == HandleSlotNormal || slot == HandleSlotAutoTask) {
        bool hasDecorator = false;
        WCTAssert(dynamic_cast<DecorativeHandle *>(handle) != nullptr);
        auto *decorativeHandle = dynamic_cast<DecorativeHandle *>(handle);
        // CompressingHandleDecorator must be added before MigratingHandleDecorator.
        if (m_compression.shouldCompress()) {
            hasDecorator = true;
            decorativeHandle->tryAddDecorator<CompressingHandleDecorator>(DecoratorCompressingHandle, m_compression);
        }
        if (type == HandleType::Normal && m_migration.shouldMigrate()) {
            hasDecorator = true;
            decorativeHandle->tryAddDecorator<MigratingHandleDecorator>(DecoratorMigratingHandle, m_migration);
        }
        if (!hasDecorator) {
            decorativeHandle->clearDecorators();
        }
    }

    Configs configs;
    {
        SharedLockGuard memoryGuard(m_memory);
        configs = m_configs;
    }
    const bool succeed = handle->reconfigure(configs);
    if (!succeed) {
        setThreadedError(handle->getError());
        return false;
    }

    if (slot != HandleSlotAssemble && slot != HandleSlotVacuum && slot != HandleSlotCipher) {
        handle->setPath(path);
        const bool hasOpened = handle->isOpened();
        const Time start = Time::now();
        const uint64_t cpuStart = Time::currentThreadCPUTimeInMicroseconds();
        // 打开数据库 获取句柄。
        if (!handle->open()) {
            setThreadedError(handle->getError());
            return false;
        }
        if (!hasOpened && slot == HandleSlotNormal) {
            const std::time_t openTime= (Time::now().nanoseconds() - start.nanoseconds()) / 1000;
            const uint64_t openCPUTime = Time::currentThreadCPUTimeInMicroseconds() - cpuStart;
            int memoryUsed, tableCount, indexCount, triggerCount;
            if (handle->getSchemaInfo(memoryUsed, tableCount, indexCount, triggerCount)) {
                StringViewMap<Value> info;
                info.insert_or_assign(MonitorInfoKeyHandleOpenTime, openTime);
                info.insert_or_assign(MonitorInfoKeyHandleOpenCPUTime, openCPUTime);
                info.insert_or_assign(MonitorInfoKeySchemaUsage, memoryUsed);
                info.insert_or_assign(MonitorInfoKeyTableCount, tableCount);
                info.insert_or_assign(MonitorInfoKeyIndexCount, indexCount);
                info.insert_or_assign(MonitorInfoKeyTriggerCount, triggerCount);
                info.insert_or_assign(MonitorInfoKeyHandleCount, numberOfAliveHandlesInSlot(slot) + 1);
                DBOperationNotifier::shared().notifyOperation(this, DBOperationNotifier::Operation::OpenHandle, info);
            }
        }
    } else if (slot == HandleSlotCipher) {
        WCTAssert(dynamic_cast<CipherHandle *>(handle) != nullptr);
        auto *cipherHandle = dynamic_cast<CipherHandle *>(handle);
        if (!cipherHandle->openCipherInMemory()) {
            setThreadedError(cipherHandle->getCipherError());
            return false;
        }
        auto salt = cipherHandle->tryGetSaltFromDatabase(getPath());
        if (salt.failed()) {
            assignWithSharedThreadedError();
            return false;
        }
        if (!salt.value().empty()) {
            cipherHandle->setCipherSalt(salt.value());
        }
    }
    return true;
}

#pragma mark - Threaded
void InnerDatabase::markHandleAsTransactioned(InnerHandle *handle)
{
    WCTAssert(m_transactionedHandles.getOrCreate().get() == nullptr);
    RecyclableHandle currentHandle = getHandle();
    WCTAssert(currentHandle.get() == handle);
    m_transactionedHandles.getOrCreate() = currentHandle;
    WCTAssert(m_transactionedHandles.getOrCreate().get() != nullptr);
}

void InnerDatabase::markHandleAsUntransactioned()
{
    WCTAssert(m_transactionedHandles.getOrCreate().get() != nullptr);
    m_transactionedHandles.getOrCreate() = nullptr;
    WCTAssert(m_transactionedHandles.getOrCreate().get() == nullptr);
}

#pragma mark - Transaction
bool InnerDatabase::isInTransaction()
{
    WCTAssert(m_transactionedHandles.getOrCreate().get() == nullptr
              || m_transactionedHandles.getOrCreate().get()->isInTransaction());
    return m_transactionedHandles.getOrCreate().get() != nullptr;
}

bool InnerDatabase::beginTransaction()
{
    RecyclableHandle handle = getHandle(true);
    if (handle == nullptr) {
        return false;
    }
    if (handle->beginTransaction()) {
        return true;
    }
    setThreadedError(handle->getError());
    return false;
}

bool InnerDatabase::commitOrRollbackTransaction()
{
    RecyclableHandle handle = getHandle();
    WCTRemedialAssert(handle != nullptr,
                      "Commit or rollback transaction should not be called without begin.",
                      return false;);
    if (handle->commitOrRollbackTransaction()) {
        return true;
    }
    setThreadedError(handle->getError());
    return false;
}

void InnerDatabase::rollbackTransaction()
{
    RecyclableHandle handle = getHandle();
    WCTRemedialAssert(handle != nullptr,
                      "Rollback transaction should not be called without begin.",
                      return;);
    handle->rollbackTransaction();
}

bool InnerDatabase::runTransaction(const TransactionCallback &transaction)
{
    RecyclableHandle handle = getHandle(true);
    if (handle == nullptr) return false;
    if (!handle->runTransaction(transaction)) {
        setThreadedError(handle->getError());
        return false;
    }
    return true;
}

bool InnerDatabase::runPausableTransactionWithOneLoop(const TransactionCallbackForOneLoop &transaction)
{
    // get threaded handle
    RecyclableHandle handle = getHandle(true);
    if (handle == nullptr) return false;
    if (!handle->runPausableTransactionWithOneLoop(transaction)) {
        setThreadedError(handle->getError());
        return false;
    }
    return true;
}

#pragma mark - File
bool InnerDatabase::removeFiles()
{
    if (m_isInMemory) {
        return false;
    }
    bool result = false;
    close([&result, this]() {
        std::list<StringView> paths = getPaths();
        paths.reverse(); // reverse to remove the non-critical paths first avoiding app stopped between the removing
        result = FileManager::removeItems(paths);
        if (!result) {
            assignWithSharedThreadedError();
        }
        m_migration.purge();
        m_compression.purge();
    });
    return result;
}

Optional<size_t> InnerDatabase::getFilesSize()
{
    if (m_isInMemory) {
        return 0;
    }
    auto size = FileManager::getItemsSize(getPaths());
    if (!size.succeed()) {
        assignWithSharedThreadedError();
    }
    return size;
}

bool InnerDatabase::moveFiles(const UnsafeStringView &directory)
{
    if (m_isInMemory) {
        return false;
    }
    bool result = false;
    close([&result, &directory, this]() {
        std::list<StringView> paths = getPaths();
        paths.reverse();
        result = FileManager::moveItems(paths, directory);
        if (!result) {
            assignWithSharedThreadedError();
        }
    });
    return result;
}

const StringView &InnerDatabase::getPath() const
{
    return path;
}

std::list<StringView> InnerDatabase::getPaths() const
{
    if (m_isInMemory) {
        return { path };
    }
    return pathsOfDatabase(path);
}

std::list<StringView> InnerDatabase::pathsOfDatabase(const UnsafeStringView &database)
{
    return {
        StringView(database),
        InnerHandle::walPathOfDatabase(database),
        Repair::Factory::incrementalMaterialPathForDatabase(database),
        Repair::Factory::firstMaterialPathForDatabase(database),
        Repair::Factory::lastMaterialPathForDatabase(database),
        Repair::Factory::factoryPathForDatabase(database),
        InnerHandle::journalPathOfDatabase(database),
        InnerHandle::shmPathOfDatabase(database),
    };
}

void InnerDatabase::setInMemory()
{
    WCTAssert(!m_initialized);
    m_isInMemory = true;
}

#pragma mark - Repair

void InnerDatabase::markNeedLoadIncremetalMaterial()
{
    m_needLoadIncremetalMaterial = true;
}

void InnerDatabase::tryLoadIncremetalMaterial()
{
    if (!m_needLoadIncremetalMaterial) {
        return;
    }

    const StringView &databasePath = getPath();
    StringView materialPath = Repair::Factory::incrementalMaterialPathForDatabase(databasePath);
    auto exist = FileManager::fileExists(materialPath);
    if (!exist.hasValue()) {
        return;
    }
    if (!exist.value()) {
        m_needLoadIncremetalMaterial = false;
        return;
    }
    SharedIncrementalMaterial material = std::make_shared<Repair::IncrementalMaterial>();
    RecyclableHandle handle = flowOut(HandleType::BackupCipher);
    if (handle == nullptr) {
        return;
    }
    WCTAssert(dynamic_cast<CipherHandle *>(handle.get()) != nullptr);
    CipherHandle *cipherHandle = static_cast<CipherHandle *>(handle.get());
    bool useMaterial = false;
    if (cipherHandle->isCipherDB()) {
        material->setCipherDelegate(cipherHandle);
        useMaterial = material->decryptedDeserialize(materialPath, false);
        material->setCipherDelegate(nullptr);
    } else {
        useMaterial = material->deserialize(materialPath);
    }
    if (useMaterial) {
        if (material->pages.size() < BackupMaxAllowIncrementalPageCount) {
            CommonCore::shared().tryRegisterIncrementalMaterial(getPath(), material);
        } else {
            FileManager::removeItem(materialPath);
            Error error(Error::Code::Error, Error::Level::Warning, "Remove large incremental material");
            error.infos.insert_or_assign(ErrorStringKeySource, ErrorSourceRepair);
            error.infos.insert_or_assign(ErrorStringKeyPath, databasePath);
            error.infos.insert_or_assign("PageCount", material->pages.size());
            Notifier::shared().notify(error);
        }
    } else {
        FileManager::removeItem(materialPath);
        Error error(Error::Code::Error, Error::Level::Warning, "Remove unresolved incremental material");
        error.infos.insert_or_assign(ErrorStringKeySource, ErrorSourceRepair);
        error.infos.insert_or_assign(ErrorStringKeyPath, databasePath);
        Notifier::shared().notify(error);
    }
    m_needLoadIncremetalMaterial = false;
}

void InnerDatabase::filterBackup(const BackupFilter &tableShouldBeBackedup)
{
    LockGuard memoryGuard(m_memory);
    m_factory.filter(tableShouldBeBackedup);
}

bool InnerDatabase::backup(bool interruptible)
{
    // 如果数据库为内存数据库，则无法备份，直接返回false
    if (m_isInMemory) {
        return false;
    }
    // 初始化数据库，确保数据库处于可用状态
    InitializedGuard initializedGuard = initialize();
    if (!initializedGuard.valid()) {
        return false; // 如果初始化失败，则返回false
    }

    // 断言：备份不能在事务处理中进行，防止数据不一致
    WCTRemedialAssert(
    !isInTransaction(), "Backup can't be run in transaction.", return false;);

    // 断言：Lite Mode下不允许备份
    WCTRemedialAssert(!m_liteModeEnable, "Backup can't run in lite mode.", return false;);

    // 获取备份操作句柄
    RecyclableHandle backupHandle = flowOut(HandleType::Backup);
    if (backupHandle == nullptr) {
        return false;
    }

    // 获取备份加密句柄
    RecyclableHandle backupCipherHandle = flowOut(HandleType::BackupCipher);
    if (backupCipherHandle == nullptr) {
        return false;
    }
    // 确保备份句柄和加密句柄不相同
    WCTAssert(backupHandle.get() != backupCipherHandle.get());

    // 如果备份允许中断，则设置备份句柄为可中断状态，并检查是否需要中断
    if (interruptible) {
        backupHandle->markAsCanBeSuspended(true);
        if (checkShouldInterruptWhenClosing(ErrorTypeBackup)) {
            return false;
        }
    }

    // 设置错误路径，便于错误追踪
    CommonCore::shared().setThreadedErrorPath(path);

    // 创建备份对象
    Repair::FactoryBackup backup = m_factory.backup();
    // 获取备份句柄的操作代理，用于处理备份相关操作
    auto &backupOperator = backupHandle.getDecorative()->getOrCreateOperator<Repair::BackupHandleOperator>(OperatorBackup);
    // 绑定共享和独占代理
    backup.setBackupSharedDelegate(&backupOperator);
    backup.setBackupExclusiveDelegate(&backupOperator);
    // 确保备份加密句柄为CipherHandle类型
    WCTAssert(dynamic_cast<CipherHandle *>(backupCipherHandle.get()) != nullptr);
    // 设置加密代理
    backup.setCipherDelegate(static_cast<CipherHandle *>(backupCipherHandle.get()));

    // 执行备份操作，返回备份是否成功
    bool succeed = backup.work(getPath(), interruptible);
    // 如果备份失败，根据错误级别决定是否忽略错误
    if (!succeed) {
        if (backup.getError().level == Error::Level::Ignore) {
            succeed = true;
        } else {
            setThreadedError(backup.getError());
        }
    }
    // 清空错误路径
    CommonCore::shared().setThreadedErrorPath("");
    // 返回备份结果
    return succeed;
}

bool InnerDatabase::deposit()
{
    if (m_isInMemory) {
        return false;
    }
    bool result = false;
    close([&result, this]() {
        InitializedGuard initializedGuard = initialize();
        if (!initializedGuard.valid()) {
            return;
        }

        RecyclableHandle backupHandle = flowOut(HandleType::AssembleBackup);
        if (backupHandle == nullptr) {
            return;
        }
        RecyclableHandle assemblerHandle = flowOut(HandleType::Assemble);
        if (assemblerHandle == nullptr) {
            return;
        }
        RecyclableHandle cipherHandle = flowOut(HandleType::AssembleCipher);
        if (cipherHandle == nullptr) {
            return;
        }
        WCTAssert(backupHandle.get() != assemblerHandle.get());
        WCTAssert(backupHandle.get() != cipherHandle.get());
        WCTAssert(assemblerHandle.get() != cipherHandle.get());

        WCTAssert(!backupHandle->isOpened());
        WCTAssert(!assemblerHandle->isOpened());

        CommonCore::shared().setThreadedErrorPath(path);

        Repair::FactoryRenewer renewer = m_factory.renewer();
        Repair::BackupHandleOperator backupOperator(backupHandle.get());
        renewer.setBackupSharedDelegate(&backupOperator);
        renewer.setBackupExclusiveDelegate(&backupOperator);
        AssembleHandleOperator assembleOperator(assemblerHandle.get());
        renewer.setAssembleDelegate(&assembleOperator);
        WCTAssert(dynamic_cast<CipherHandle *>(cipherHandle.get()) != nullptr);
        renewer.setCipherDelegate(static_cast<CipherHandle *>(cipherHandle.get()));
        // Prepare a new database from material at renew directory and wait for moving
        if (!renewer.prepare()) {
            setThreadedError(renewer.getError());
            CommonCore::shared().setThreadedErrorPath("");
            return;
        }
        Repair::FactoryDepositor depositor = m_factory.depositor();
        if (!depositor.work()) {
            setThreadedError(depositor.getError());
            return;
        }
        // If app stop here, it results that the old database is moved to deposited directory and the renewed one is not moved to the origin directory.
        // At next time this database launchs, the retrieveRenewed method will do the remaining work. So data will never lost.
        if (!renewer.work()) {
            setThreadedError(renewer.getError());
            CommonCore::shared().setThreadedErrorPath("");
        } else {
            result = true;
        }
        cipherHandle->close();
    });
    CommonCore::shared().setThreadedErrorPath("");
    return result;
}

bool InnerDatabase::containsDeposited() const
{
    SharedLockGuard concurrencyGuard(m_concurrency);
    return m_factory.containsDeposited();
}

bool InnerDatabase::removeDeposited()
{
    bool result = false;
    close([&result, this]() {
        result = m_factory.removeDeposited();
        if (!result) {
            assignWithSharedThreadedError();
        }
    });
    return result;
}

double InnerDatabase::retrieve(const ProgressCallback &onProgressUpdated)
{
    if (m_isInMemory) {
        return 0;
    }
    double result = -1;
    close([&result, &onProgressUpdated, this] {
        // 1. 初始化数据库，确保数据库处于可用状态
        InitializedGuard initializedGuard = initialize();
        // 如果初始化失败，则直接返回，不进行恢复操作
        if (!initializedGuard.valid()) {
            return;
        }
        // 2. 获取备份操作句柄，用于读取备份数据
        RecyclableHandle backupHandle = flowOut(HandleType::AssembleBackup);
        // 如果备份句柄获取失败，则无法进行恢复，直接返回
        if (backupHandle == nullptr) {
            return;
        }
        // 3. 获取装配句柄，用于重建数据库文件
        RecyclableHandle assemblerHandle = flowOut(HandleType::Assemble);
        // 如果装配句柄获取失败，则无法进行恢复，直接返回
        if (assemblerHandle == nullptr) {
            return;
        }
        // 4. 标记装配句柄为可中断，允许恢复过程被暂停
        assemblerHandle->markAsCanBeSuspended(true);
        // 5. 获取加密句柄，用于处理数据库加密解密
        RecyclableHandle cipherHandle = flowOut(HandleType::AssembleCipher);
        // 如果加密句柄获取失败，则无法进行恢复，直接返回
        if (cipherHandle == nullptr) {
            return;
        }
        // 6. 确保备份、装配和加密句柄是不同的对象，避免操作冲突
        WCTAssert(backupHandle.get() != assemblerHandle.get());
        // 6.1 检查备份句柄与加密句柄是否相同
        WCTAssert(backupHandle.get() != cipherHandle.get());
        // 6.2 检查装配句柄与加密句柄是否相同
        WCTAssert(assemblerHandle.get() != cipherHandle.get());
        // 7. 确保备份和装配句柄尚未打开，防止干扰恢复过程
        WCTAssert(!backupHandle->isOpened());
        // 7.1 检查装配句柄是否已打开
        WCTAssert(!assemblerHandle->isOpened());
        // 8. 设置错误路径，便于在恢复过程中追踪错误
        CommonCore::shared().setThreadedErrorPath(path);
        // 9. 创建恢复操作对象，负责执行恢复逻辑
        Repair::FactoryRetriever retriever = m_factory.retriever();
        // 10. 设置备份代理，绑定备份句柄
        Repair::BackupHandleOperator backupOperator(backupHandle.get());
        // 10.1 绑定共享备份代理
        retriever.setBackupSharedDelegate(&backupOperator);
        // 10.2 绑定独占备份代理
        retriever.setBackupExclusiveDelegate(&backupOperator);
        // 11. 设置装配代理，绑定装配句柄
        AssembleHandleOperator assembleOperator(assemblerHandle.get());
        // 11.1 绑定装配代理
        retriever.setAssembleDelegate(&assembleOperator);
        // 12. 确保加密句柄为 CipherHandle 类型
        WCTAssert(dynamic_cast<CipherHandle *>(cipherHandle.get()) != nullptr);
        // 12.1 设置加密代理，绑定加密句柄
        retriever.setCipherDelegate(dynamic_cast<CipherHandle *>(cipherHandle.get()));
        // 13. 设置恢复进度回调，实时反馈恢复进度
        retriever.setProgressCallback(onProgressUpdated);
        // 14. 执行恢复操作，如果成功则获取恢复评分
        if (retriever.work()) {
            // 14.1 获取恢复后的评分，反映数据恢复的完整性
            result = retriever.getScore().value();
        }
        // 15. 记录恢复过程中发生的错误（如果有）
        setThreadedError(retriever.getError()); // retriever may have non-critical error even if it succeeds.
        // 16. 清空错误路径，结束错误追踪
        CommonCore::shared().setThreadedErrorPath("");
        // 17. 关闭加密句柄，释放资源
        cipherHandle->close();
    });
    return result;
}

bool InnerDatabase::removeMaterials()
{
    bool result = false;
    close([&result, this]() {
        result = FileManager::removeItems(
        { Repair::Factory::incrementalMaterialPathForDatabase(path),
          Repair::Factory::firstMaterialPathForDatabase(path),
          Repair::Factory::lastMaterialPathForDatabase(path) });
        if (!result) {
            assignWithSharedThreadedError();
        }
    });
    return result;
}

void InnerDatabase::checkIntegrity(bool interruptible)
{
    InitializedGuard initializedGuard = initialize();
    if (!initializedGuard.valid()) {
        return; // mark as succeed if it's not an auto initialize action.
    }
    RecyclableHandle handle = flowOut(HandleType::IntegrityCheck);
    if (handle != nullptr) {
        auto &integerityOperator = handle.getDecorative()->getOrCreateOperator<IntegerityHandleOperator>(OperatorCheckIntegrity);
        if (interruptible) {
            if (checkShouldInterruptWhenClosing(ErrorTypeIntegrity)) {
                return;
            }
            handle->markAsCanBeSuspended(true);
        }
        integerityOperator.checkIntegrity();
    }
}

#pragma mark - Vacuum

bool InnerDatabase::vacuum(const ProgressCallback &onProgressUpdated)
{
    if (m_isInMemory) {
        return true;
    }
    bool result = false;
    close([&result, &onProgressUpdated, this]() {
        InitializedGuard initializedGuard = initialize();
        if (!initializedGuard.valid()) {
            return;
        }

        RecyclableHandle vacuumHandle = flowOut(HandleType::Vacuum);
        if (vacuumHandle == nullptr) {
            return;
        }

        CommonCore::shared().setThreadedErrorPath(path);

        Repair::FactoryVacuum vacuummer = m_factory.vacuumer();
        VacuumHandleOperator vacuumOperator(vacuumHandle.get());
        vacuummer.setVacuumDelegate(&vacuumOperator);
        vacuummer.setProgressCallback(onProgressUpdated);

        if (!vacuummer.prepare()) {
            setThreadedError(vacuummer.getError());
            CommonCore::shared().setThreadedErrorPath("");
            return;
        }

        if (!vacuummer.work()) {
            setThreadedError(vacuummer.getError());
            CommonCore::shared().setThreadedErrorPath("");
            return;
        }
        CommonCore::shared().setThreadedErrorPath("");
        result = true;
    });
    return result;
}

void InnerDatabase::enableAutoVacuum(bool incremental)
{
    setConfig(AutoVacuumConfigName,
              std::static_pointer_cast<WCDB::Config>(
              std::make_shared<WCDB::AutoVacuumConfig>(incremental)),
              Configs::Priority::Highest);
}

bool InnerDatabase::incrementalVacuum(int pages)
{
    InitializedGuard initializedGuard = initialize();
    if (!initializedGuard.valid()) {
        return false; // mark as succeed if it's not an auto initialize action.
    }
    RecyclableHandle handle = flowOut(HandleType::AutoVacuum);
    if (!handle->prepare(StatementPragma().pragma(Pragma::incrementalVacuum()).with(pages))) {
        return false;
    }
    bool succeed = false;
    do {
        succeed = handle->step();
    } while (succeed && !handle->done());
    handle->finalize();
    return succeed;
}

#pragma mark - Migration
Optional<bool> InnerDatabase::stepMigration(bool interruptible)
{
    InitializedGuard initializedGuard = initialize();
    if (!initializedGuard.valid()) {
        return NullOpt;
    }
    WCTRemedialAssert(
    !isInTransaction(), "Migrating can't be run in transaction.", return NullOpt;);
    WCTRemedialAssert(
    m_migration.shouldMigrate(), "It's not configured for migration.", return NullOpt;);
    Optional<bool> done;
    RecyclableHandle handle = flowOut(HandleType::Migrate);
    if (handle != nullptr) {
        MigrateHandleOperator &migrateOperator
        = handle.getDecorative()->getOrCreateOperator<MigrateHandleOperator>(OperatorMigrate);
        if (interruptible) {
            if (checkShouldInterruptWhenClosing(ErrorTypeMigrate)) {
                return false;
            }
            handle->markAsCanBeSuspended(true);
        }
        handle->markErrorAsIgnorable(Error::Code::Busy);

        done = m_migration.step(migrateOperator);
        if (!done.succeed() && handle->getError().isIgnorable()) {
            done = false;
        }
    }
    return done;
}

void InnerDatabase::didMigrate(const MigrationBaseInfo *info)
{
    MigratedCallback callback = nullptr;
    {
        SharedLockGuard lockGuard(m_memory);
        callback = m_migratedCallback;
    }
    if (callback != nullptr) {
        callback(this, info);
    }
}

void InnerDatabase::setNotificationWhenMigrated(const MigratedCallback &callback)
{
    LockGuard lockGuard(m_memory);
    m_migratedCallback = callback;
}

void InnerDatabase::addMigration(const UnsafeStringView &sourcePath,
                                 const UnsafeData &sourceCipher,
                                 const MigrationTableFilter &filter)
{
    StringView sourceDatabase = Path::normalize(sourcePath);
    if (sourceDatabase.compare(getPath()) != 0) {
        close([=]() {
            m_migration.addMigration(sourceDatabase, sourceCipher, filter);
        });
    } else {
        close([=]() {
            m_migration.addMigration(UnsafeStringView(), sourceCipher, filter);
        });
    }
}

bool InnerDatabase::isMigrated() const
{
    return m_migration.isMigrated();
}

StringViewSet InnerDatabase::getPathsOfSourceDatabases() const
{
    return m_migration.getPathsOfSourceDatabases();
}

#pragma mark - Compression
Optional<bool> InnerDatabase::stepCompression(bool interruptible)
{
    InitializedGuard initializedGuard = initialize();
    if (!initializedGuard.valid()) {
        return NullOpt;
    }
    WCTRemedialAssert(
    !isInTransaction(), "Compressing can't be run in transaction.", return NullOpt;);
    WCTRemedialAssert(m_compression.shouldCompress(),
                      "It's not configured for compression.",
                      return NullOpt;);
    Optional<bool> done;
    RecyclableHandle handle = flowOut(HandleType::Compress);
    if (handle != nullptr) {
        CompressHandleOperator &compressOperator
        = handle.getDecorative()->getOrCreateOperator<CompressHandleOperator>(OperatorCompress);
        if (interruptible) {
            if (checkShouldInterruptWhenClosing(ErrorTypeCompress)) {
                return false;
            }
            handle->markAsCanBeSuspended(true);
        }
        handle->markErrorAsIgnorable(Error::Code::Busy);

        done = m_compression.step(compressOperator);
        if (!done.succeed() && handle->getError().isIgnorable()) {
            done = false;
        }
    }
    return done;
}

void InnerDatabase::didCompress(const CompressionTableBaseInfo *info)
{
    CompressedCallback callback = nullptr;
    {
        SharedLockGuard lockGuard(m_memory);
        callback = m_compressedCallback;
    }
    if (callback != nullptr) {
        callback(this, info);
    }
}

void InnerDatabase::setNotificationWhenCompressed(const CompressedCallback &callback)
{
    LockGuard lockGuard(m_memory);
    m_compressedCallback = callback;
}

void InnerDatabase::addCompression(const CompressionTableFilter &filter)
{
    close([=]() { m_compression.setTableFilter(filter); });
}

void InnerDatabase::setCanCompressNewData(bool canCompress)
{
    m_compression.setCanCompressNewData(canCompress);
}

bool InnerDatabase::isCompressed() const
{
    return m_compression.isCompressed();
}

bool InnerDatabase::rollbackCompression(const ProgressCallback &callback)
{
    WCTRemedialAssert(
    !isInTransaction(), "Can't revert compression in transaction.", return false;);

    bool ret = false;
    close([&]() {
        InitializedGuard initializedGuard = initialize();
        if (!initializedGuard.valid()) {
            return; // mark as succeed if it's not an auto initialize action.
        }

        m_compression.setProgressCallback(callback);

        RecyclableHandle handle = flowOut(HandleType::Compress);
        if (handle != nullptr) {
            CompressHandleOperator &compressOperator
            = handle.getDecorative()->getOrCreateOperator<CompressHandleOperator>(OperatorCompress);

            ret = m_compression.rollbackCompression(compressOperator);
            if (ret) {
                CommonCore::shared().enableAutoCompress(this, false);
            }
        }
    });
    return ret;
}

#pragma mark - Checkpoint
bool InnerDatabase::checkpoint(bool interruptible, CheckPointMode mode)
{
    InitializedGuard initializedGuard = initialize();
    if (!initializedGuard.valid()) {
        return false; // mark as succeed if it's not an auto initialize action.
    }
    bool succeed = false;
    const RecyclableHandle handle = flowOut(HandleType::Checkpoint);
    if (handle != nullptr) {
        if (interruptible) {
            if (checkShouldInterruptWhenClosing(ErrorTypeCheckpoint)) {
                return false;
            }
            handle->markAsCanBeSuspended(true);
        }
        tryLoadIncremetalMaterial();
        handle->markErrorAsIgnorable(Error::Code::Busy);
        succeed = handle->checkpoint(mode);
        if (!succeed && handle->getError().isIgnorable()) {
            succeed = true;
        }
    }
    return succeed;
}

#pragma mark - AutoMergeFTSIndex

Optional<bool> InnerDatabase::mergeFTSIndex(TableArray newTables, TableArray modifiedTables)
{
    InitializedGuard initializedGuard = initialize();
    if (!initializedGuard.valid()) {
        return false; // mark as succeed if it's not an auto initialize action.
    }
    if (checkShouldInterruptWhenClosing(ErrorTypeMergeIndex)) {
        return false;
    }
    return m_mergeLogic.triggerMerge(newTables, modifiedTables);
}

void InnerDatabase::proccessMerge()
{
    InitializedGuard initializedGuard = initialize();
    if (!initializedGuard.valid()) {
        return; // mark as succeed if it's not an auto initialize action.
    }
    if (checkShouldInterruptWhenClosing(ErrorTypeMergeIndex)) {
        return;
    }
    return m_mergeLogic.proccessMerge();
}

RecyclableHandle InnerDatabase::getMergeIndexHandle()
{
    return flowOut(HandleType::MergeIndex);
}

} //namespace WCDB
