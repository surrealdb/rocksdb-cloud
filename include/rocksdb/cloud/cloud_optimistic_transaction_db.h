//  Copyright (c) 2024-present, SurrealDB Inc

#pragma once

#include <string>
#include <vector>

#include "rocksdb/cloud/db_cloud.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"

namespace ROCKSDB_NAMESPACE {
//
// OptimisticTransactionDB with Cloud support.
//
// Important: The caller is responsible for ensuring that only one database at
// a time is running with the same cloud destination bucket and path. Running
// two databases concurrently with the same destination path will lead to
// corruption if it lasts for more than couple of minutes.
class CloudOptimisticTransactionDB : public DBCloud {
 public:
  static Status Open(const Options& options, const std::string& name,
                     const std::string& persistent_cache_path,
                     const uint64_t persistent_cache_size_gb,
                     CloudOptimisticTransactionDB** dbptr,
                     const OptimisticTransactionDBOptions& occ_options =
                         OptimisticTransactionDBOptions());

  static Status Open(const Options& options, const std::string& dbname,
                     const std::vector<ColumnFamilyDescriptor>& column_families,
                     const std::string& persistent_cache_path,
                     const uint64_t persistent_cache_size_gb,
                     std::vector<ColumnFamilyHandle*>* handles,
                     CloudOptimisticTransactionDB** dbptr,
                     const OptimisticTransactionDBOptions& occ_options =
                         OptimisticTransactionDBOptions());

  virtual OptimisticTransactionDB* GetTxnDB() = 0;

  virtual ~CloudOptimisticTransactionDB() {}

 protected:
  explicit CloudOptimisticTransactionDB(std::shared_ptr<DB> db) : DBCloud(db) {}
};

}  // namespace ROCKSDB_NAMESPACE
