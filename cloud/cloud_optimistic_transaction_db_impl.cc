// Copyright (c) 2024 SurrealDB Inc.
#ifndef ROCKSDB_LITE

#include "cloud/cloud_optimistic_transaction_db_impl.h"

#include <aws/core/Aws.h>

#include <cinttypes>

#include "cloud/cloud_log_controller_impl.h"
#include "cloud/cloud_manifest.h"
#include "cloud/db_cloud_impl.h"
#include "cloud/filename.h"
#include "cloud/manifest_reader.h"
#include "env/composite_env_wrapper.h"
#include "file/file_util.h"
#include "file/sst_file_manager_impl.h"
#include "logging/auto_roll_logger.h"
#include "rocksdb/cloud/cloud_file_system_impl.h"
#include "rocksdb/cloud/cloud_log_controller.h"
#include "rocksdb/cloud/cloud_storage_provider.h"
#include "rocksdb/db.h"
#include "rocksdb/env.h"
#include "rocksdb/options.h"
#include "rocksdb/persistent_cache.h"
#include "rocksdb/status.h"
#include "rocksdb/table.h"
#include "util/xxhash.h"
#include "utilities/persistent_cache/block_cache_tier.h"
#include "utilities/transactions/optimistic_transaction_db_impl.h"

namespace ROCKSDB_NAMESPACE {
Status CloudOptimisticTransactionDB::Open(
    const Options& options, const std::string& name,
    const std::string& persistent_cache_path,
    const uint64_t persistent_cache_size_gb,
    CloudOptimisticTransactionDB** dbptr,
    const OptimisticTransactionDBOptions& occ_options) {
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;

  Status s = CloudOptimisticTransactionDB::Open(
      options, name, column_families, persistent_cache_path,
      persistent_cache_size_gb, &handles, dbptr, occ_options);
  if (s.ok()) {
    assert(handles.size() == 1);
    // the handle can be deleted since DBImpl is always holding a reference to
    // default column family
    delete handles[0];
  }

  return s;
}

Status CloudOptimisticTransactionDB::Open(
    const Options& opts, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    const std::string& persistent_cache_path,
    const uint64_t persistent_cache_size_gb,
    std::vector<ColumnFamilyHandle*>* handles,
    CloudOptimisticTransactionDB** dbptr,
    const OptimisticTransactionDBOptions& occ_options) {
  Status st;
  std::string dbid;
  std::vector<ColumnFamilyDescriptor> column_families_copy = column_families;

  /* Copied from OptimisticTransactionDB */
  // Enable MemTable History if not already enabled
  for (auto& column_family : column_families_copy) {
    ColumnFamilyOptions* options = &column_family.options;

    if (options->max_write_buffer_size_to_maintain == 0 &&
        options->max_write_buffer_number_to_maintain == 0) {
      // Setting to -1 will set the History size to
      // max_write_buffer_number * write_buffer_size.
      options->max_write_buffer_size_to_maintain = -1;
    }
  }

  DBCloud* db = nullptr;
  st = DBCloud::Open(opts, dbname, column_families, persistent_cache_path,
                     persistent_cache_size_gb, handles, &db, false);

  if (st.ok()) {
    *dbptr = new CloudOptimisticTransactionDBImpl(
        static_cast_with_check<DBCloudImpl>(db), occ_options);

    db->GetDbIdentity(dbid);
  }
  Log(InfoLogLevel::INFO_LEVEL, db->GetOptions().info_log,
      "Opened Optimistic Transaction Cloud db with local dir %s dbid %s. %s",
      db->GetName().c_str(), dbid.c_str(), st.ToString().c_str());
  return st;
};

Status CloudOptimisticTransactionDBImpl::Savepoint() {
  return db_cloud_->Savepoint();
}

Status CloudOptimisticTransactionDBImpl::CheckpointToCloud(
    const BucketOptions& destination, const CheckpointToCloudOptions& options) {
  return db_cloud_->CheckpointToCloud(destination, options);
}

}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
