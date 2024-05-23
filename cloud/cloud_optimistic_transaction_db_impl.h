// Copyright (c) 2024 SurrealDB Inc.

#pragma once

#ifndef ROCKSDB_LITE
#include <rocksdb/utilities/transactions/optimistic_transaction_db_impl.h>

#include <deque>
#include <memory>
#include <string>
#include <vector>

#include "port/port.h"
#include "rocksdb/cloud/cloud_optimistic_transaction_db.h"
#include "rocksdb/cloud/db_cloud_impl.h"
#include "rocksdb/db.h"

namespace ROCKSDB_NAMESPACE {
class Env;

//
// All writes to this OptimisticTransactionDB are configured to be persisted
// in cloud storage.
//
class CloudOptimisticTransactionDBImpl : public CloudOptimisticTransactionDB {
 public:
  ~CloudOptimisticTransactionDBImpl(){};

  explicit CloudOptimisticTransactionDBImpl(
      DBCloudImpl* db_cloud, const OptimisticTransactionDBOptions& txn_opts)
      : CloudOptimisticTransactionDB(
            std::shared_ptr<DB>(db_cloud->GetBaseDB())),
        db_cloud_(db_cloud) {
    txn_db_ = new OptimisticTransactionDBImpl(db_cloud->GetBaseDB(), txn_opts,
                                              /*take_ownership=*/false);
  }

  OptimisticTransactionDB* GetTxnDB() override { return txn_db_; }

  /* DBCloud */
  Status Savepoint() override;

  Status CheckpointToCloud(const BucketOptions& destination,
                           const CheckpointToCloudOptions& options) override;

 private:
  DBCloudImpl* db_cloud_;
  OptimisticTransactionDBImpl* txn_db_;
};
}  // namespace ROCKSDB_NAMESPACE
#endif  // ROCKSDB_LITE
