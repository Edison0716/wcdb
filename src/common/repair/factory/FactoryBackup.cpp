//
// Created by sanhuazhang on 2018/06/05
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

#include "FactoryBackup.hpp"
#include "CommonCore.hpp"
#include "CoreConst.h"
#include "Data.hpp"
#include "Factory.hpp"
#include "FileManager.hpp"
#include "Notifier.hpp"

namespace WCDB {

namespace Repair {

// 析构函数，默认实现
FactoryBackup::~FactoryBackup() = default;

/**
 * @brief 备份数据库的主要函数
 * @param database 需要备份的数据库路径
 * @param interruptible 是否支持中断
 * @return 是否成功
 */
bool FactoryBackup::work(const UnsafeStringView& database, bool interruptible)
{
    // 设置共享和独占备份代理的数据库路径
    m_sharedDelegate->setBackupPath(database);
    m_exclusiveDelegate->setBackupPath(database);

    // 获取备份锁，防止备份过程中触发 checkpoint
    if (!m_sharedDelegate->acquireBackupSharedLock()) {
        return false; // 锁获取失败，返回false
    }

    // 执行实际备份逻辑
    bool succeed = doBackUp(database, interruptible);

    // 释放备份锁，确保备份成功时不影响后续操作
    if (!m_sharedDelegate->releaseBackupSharedLock() && succeed) {
        setError(m_sharedDelegate->getBackupError()); // 如果释放锁失败且备份成功，设置错误
        return false; // 返回false
    }

    return succeed; // 返回备份结果
}

/**
 * @brief 实际执行备份的函数
 * @param database 需要备份的数据库路径
 * @param interruptible 是否支持中断
 * @return 是否成功
 */
bool FactoryBackup::doBackUp(const UnsafeStringView& database, bool interruptible)
{
    Optional<size_t> incrementalMaterialSize = 0; // 用于存储增量备份的大小
    SharedIncrementalMaterial incrementalMaterial;

    // 获取增量备份信息（如果有）
    incrementalMaterial = CommonCore::shared().tryGetIncrementalMaterial(database);
    if (interruptible && incrementalMaterial != nullptr) {
        incrementalMaterialSize = saveIncrementalMaterial(database, incrementalMaterial); // 保存增量备份
        if (!incrementalMaterialSize.hasValue()) {
            return false; // 如果保存失败，返回false
        }

        // 检查上次备份时间，若时间间隔过短且增量数据量小，则跳过本次备份
        if (Time::now().seconds() - incrementalMaterial->info.lastBackupTime < OperationQueueTimeIntervalForBackup
            && incrementalMaterial->pages.size() < BackupMaxIncrementalPageCount) {
            return true; // 符合条件，返回true
        }
    }

    // 备份开始时，通知系统
    if (interruptible) {
        notifiyBackupBegin(database);
    }

    // 初始化备份对象
    Backup backup(database);
    backup.setCipherDelegate(m_cipherDelegate);             // 设置加密代理
    backup.setBackupSharedDelegate(m_sharedDelegate);       // 设置共享备份代理
    backup.setBackupExclusiveDelegate(m_exclusiveDelegate); // 设置独占备份代理
    backup.filter(factory.getFilter());  // 设置过滤器

    // 执行备份任务
    if (!backup.work(incrementalMaterial)) {
        // 若错误码为数据库为空，则视为成功
        if (backup.getError().code() == Error::Code::Empty) {
            // 数据库为空，视为成功
            notifiyBackupEnd(database, 0, 0, backup.getMaterial(), incrementalMaterial);
            return true;
        }
        setError(backup.getError()); // 设置错误
        return false; // 返回false
    }

    // 备份完成后保存物理数据
    const Material& material = backup.getMaterial(); // 获取备份材料
    auto materialSize = saveMaterial(database, material); // 保存材料
    if (!materialSize.hasValue()) {
        return false; // 保存失败，返回false
    }

    // 备份完成后保存增量数据
    SharedIncrementalMaterial newIncrementalMaterial = backup.getIncrementalMaterial(); // 获取新的增量材料
    if (!saveIncrementalMaterial(database, newIncrementalMaterial).hasValue()) {
        return false; // 保存失败，返回false
    }
    CommonCore::shared().tryRegisterIncrementalMaterial(database, newIncrementalMaterial); // 注册增量材料

    // 备份结束时，通知系统
    if (interruptible) {
        notifiyBackupEnd(database, materialSize.value(), incrementalMaterialSize.value(), material, newIncrementalMaterial);
    }
    return true; // 返回true
}

/**
 * @brief 保存增量备份数据
 * @param database 数据库路径
 * @param material 需要保存的增量备份数据
 * @return 备份文件大小（可选）
 */
Optional<size_t>
FactoryBackup::saveIncrementalMaterial(const UnsafeStringView& database,
                                       SharedIncrementalMaterial material)
{
    WCTAssert(material != nullptr); // 确保增量材料不为空
    if (material == nullptr) {
        return 0; // 如果为空，返回0
    }
    StringView materialPath = Repair::Factory::incrementalMaterialPathForDatabase(database); // 获取增量材料路径
    bool succeed = false;

    // 若数据库使用加密方式，则对增量数据加密后保存
    if (m_cipherDelegate->isCipherDB()) {
        material->setCipherDelegate(m_cipherDelegate); // 设置加密代理
        StringView salt = m_cipherDelegate->tryGetSaltFromDatabase(database).value(); // 获取盐值
        succeed = material->encryptedSerialize(materialPath); // 加密并保存增量材料
        material->setCipherDelegate(nullptr); // 清除加密代理
    } else {
        succeed = material->serialize(materialPath); // 直接保存增量材料
    }

    if (!succeed) {
        return NullOpt; // 保存失败，返回NullOpt
    }

    return FileManager::getFileSize(materialPath); // 返回文件大小
}

/**
 * @brief 保存完整备份数据
 * @param database 数据库路径
 * @param material 需要保存的完整备份数据
 * @return 备份文件大小（可选）
 */
Optional<size_t>
FactoryBackup::saveMaterial(const UnsafeStringView& database, const Material& material)
{
    auto materialPath = Factory::materialForSerializingForDatabase(database); // 获取材料路径
    if (!materialPath.succeed()) {
        assignWithSharedThreadedError(); // 获取路径失败，赋值错误
        return NullOpt; // 返回NullOpt
    }

    bool succeed = false;
    if (!m_cipherDelegate->isCipherDB()) {
        succeed = material.serialize(materialPath.value()); // 保存材料
    } else {
        succeed = material.encryptedSerialize(materialPath.value()); // 加密并保存材料
    }

    if (!succeed) {
        assignWithSharedThreadedError(); // 保存失败，赋值错误
        return NullOpt; // 返回NullOpt
    }

    return FileManager::getFileSize(materialPath.value()); // 返回文件大小
}

/**
 * @brief 备份开始时发送通知
 * @param database 数据库路径
 */
void FactoryBackup::notifiyBackupBegin(const UnsafeStringView& database)
{
    Error error(Error::Code::Notice, Error::Level::Notice, "Backup Begin."); // 创建通知错误对象
    error.infos.insert_or_assign(ErrorStringKeyPath, database); // 插入数据库路径信息
    Notifier::shared().notify(error); // 发送通知
}

/**
 * @brief 备份结束时发送通知
 * @param database 数据库路径
 * @param materialSize 备份文件大小
 * @param incrementalMaterialSize 增量备份文件大小
 * @param material 备份数据
 * @param incrementalMaterial 增量备份数据
 */
void FactoryBackup::notifiyBackupEnd(const UnsafeStringView& database,
                                     size_t materialSize,
                                     size_t incrementalMaterialSize,
                                     const Material& material,
                                     SharedIncrementalMaterial incrementalMaterial)
{
    uint32_t associatedTableCount = 0; // 关联表计数
    uint32_t leafPageCount = 0; // 叶子页面计数

    // 统计备份内容的表数量和叶子节点数量
    for (auto& content : material.contentsMap) {
        associatedTableCount += content.second->associatedSQLs.size(); // 统计关联SQL数量
        leafPageCount += content.second->verifiedPagenos.size(); // 统计叶子页面数量
    }

    // 发送备份结束通知
    Error error(Error::Code::Notice, Error::Level::Notice, "Backup End."); // 创建结束通知错误对象
    error.infos.insert_or_assign("Incremental", incrementalMaterial != nullptr
                                 && incrementalMaterial->info.incrementalBackupTimes > 0); // 插入增量备份信息
    error.infos.insert_or_assign("MaterialSize", materialSize); // 插入材料大小信息
    error.infos.insert_or_assign("LastIncrementalMaterialSize", incrementalMaterialSize); // 插入上次增量材料大小信息
    error.infos.insert_or_assign("TableCount", material.contentsMap.size()); // 插入表数量信息
    error.infos.insert_or_assign("AssociatedTableCount", associatedTableCount); // 插入关联表数量信息
    error.infos.insert_or_assign("LeafPageCount", leafPageCount); // 插入叶子页面数量信息
    error.infos.insert_or_assign(ErrorStringKeyPath, database); // 插入数据库路径信息
    Notifier::shared().notify(error); // 发送通知
}

} // namespace Repair

} // namespace WCDB
