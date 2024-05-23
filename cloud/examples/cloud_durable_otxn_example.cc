// Copyright (c) 2024-present, SurrealDB, Inc.  All rights reserved.

#include <cstdio>
#include <iostream>
#include <string>

#include "rocksdb/cloud/cloud_optimistic_transaction_db.h"
#include "rocksdb/options.h"
#include "rocksdb/utilities/optimistic_transaction_db.h"
#include "rocksdb/utilities/transaction_db.h"

using namespace ROCKSDB_NAMESPACE;

// This is the local directory where the db is stored.
std::string kDBPath = "/tmp/rocksdb_cloud_durable_otxn_example";

// This is the name of the cloud storage bucket where the db
// is made durable. if you are using AWS, you have to manually
// ensure that this bucket name is unique to you and does not
// conflict with any other S3 users who might have already created
// this bucket name.
std::string kBucketSuffix = "cloud.durable.example.";
std::string kRegion = "us-west-2";

static const bool flushAtEnd = true;
static const bool disableWAL = false;

int main() {
  // cloud environment config options here
  CloudFileSystemOptions cloud_fs_options;

  // Store a reference to a cloud file system. A new cloud env object should
  // be associated with every new cloud-db.
  std::shared_ptr<FileSystem> cloud_fs;

  cloud_fs_options.credentials.InitializeSimple(
      getenv("AWS_ACCESS_KEY_ID"), getenv("AWS_SECRET_ACCESS_KEY"));
  if (!cloud_fs_options.credentials.HasValid().ok()) {
    fprintf(
        stderr,
        "Please set env variables "
        "AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY with cloud credentials");
    return -1;
  }

  // Append the user name to the bucket name in an attempt to make it
  // globally unique. S3 bucket-names need to be globally unique.
  // If you want to rerun this example, then unique user-name suffix here.
  char* user = getenv("USER");
  kBucketSuffix.append(user);

  // "rockset." is the default bucket prefix
  const std::string bucketPrefix = "rockset.";
  cloud_fs_options.src_bucket.SetBucketName(kBucketSuffix, bucketPrefix);
  cloud_fs_options.dest_bucket.SetBucketName(kBucketSuffix, bucketPrefix);

  // create a bucket name for debugging purposes
  const std::string bucketName = bucketPrefix + kBucketSuffix;

  // Create a new AWS cloud env Status
  CloudFileSystem* cfs;
  Status s = CloudFileSystemEnv::NewAwsFileSystem(
      FileSystem::Default(), kBucketSuffix, kDBPath, kRegion, kBucketSuffix,
      kDBPath, kRegion, cloud_fs_options, nullptr, &cfs);
  if (!s.ok()) {
    fprintf(stderr, "Unable to create cloud env in bucket %s. %s\n",
            bucketName.c_str(), s.ToString().c_str());
    return -1;
  }

  cloud_fs.reset(cfs);

  // Create options and use the AWS file system that we created earlier
  auto cloud_env = NewCompositeEnv(cloud_fs);
  Options options;
  options.env = cloud_env.get();
  options.create_if_missing = true;

  // No persistent read-cache
  std::string persistent_cache = "";

  // options for each write
  WriteOptions wopt;
  wopt.disableWAL = disableWAL;

  // open DB
  CloudOptimisticTransactionDB* cloud_db;
  s = CloudOptimisticTransactionDB::Open(options, kDBPath, persistent_cache, 0,
                                         &cloud_db);

  if (!s.ok()) {
    fprintf(stderr, "Unable to open db at path %s with bucket %s. %s\n",
            kDBPath.c_str(), bucketName.c_str(), s.ToString().c_str());
    return -1;
  }

  OptimisticTransactionDB* txn_db(cloud_db->GetTxnDB());
  DB* db(txn_db->GetBaseDB());

  WriteOptions write_options;
  ReadOptions read_options;
  OptimisticTransactionOptions txn_options;
  std::string value;
  ////////////////////////////////////////////////////////
  //
  // Simple OptimisticTransaction Example ("Read Committed")
  //
  ////////////////////////////////////////////////////////

  // Start a transaction
  Transaction* txn = txn_db->BeginTransaction(write_options);
  assert(txn);

  // Read a key in this transaction
  s = txn->Get(read_options, "abc", &value);
  assert(s.IsNotFound());

  // Write a key in this transaction
  s = txn->Put("abc", "xyz");
  assert(s.ok());

  // Read a key OUTSIDE this transaction. Does not affect txn.
  s = db->Get(read_options, "abc", &value);
  assert(s.IsNotFound());

  // Write a key OUTSIDE of this transaction.
  // Does not affect txn since this is an unrelated key.  If we wrote key 'abc'
  // here, the transaction would fail to commit.
  s = db->Put(write_options, "xyz", "zzz");
  assert(s.ok());
  s = db->Put(write_options, "abc", "def");
  assert(s.ok());

  // Commit transaction
  s = txn->Commit();
  assert(s.IsBusy());
  delete txn;

  s = db->Get(read_options, "xyz", &value);
  assert(s.ok());
  assert(value == "zzz");

  s = db->Get(read_options, "abc", &value);
  assert(s.ok());
  assert(value == "def");

  ////////////////////////////////////////////////////////
  //
  // "Repeatable Read" (Snapshot Isolation) Example
  //   -- Using a single Snapshot
  //
  ////////////////////////////////////////////////////////

  // Set a snapshot at start of transaction by setting set_snapshot=true
  txn_options.set_snapshot = true;
  txn = txn_db->BeginTransaction(write_options, txn_options);

  const Snapshot* snapshot = txn->GetSnapshot();

  // Write a key OUTSIDE of transaction
  s = db->Put(write_options, "abc", "xyz");
  assert(s.ok());

  // Read a key using the snapshot
  read_options.snapshot = snapshot;
  s = txn->GetForUpdate(read_options, "abc", &value);
  assert(s.ok());
  assert(value == "def");

  // Attempt to commit transaction
  s = txn->Commit();

  // Transaction could not commit since the write outside of the txn conflicted
  // with the read!
  assert(s.IsBusy());

  delete txn;
  // Clear snapshot from read options since it is no longer valid
  read_options.snapshot = nullptr;
  snapshot = nullptr;

  s = db->Get(read_options, "abc", &value);
  assert(s.ok());
  assert(value == "xyz");

  ////////////////////////////////////////////////////////
  //
  // "Read Committed" (Monotonic Atomic Views) Example
  //   --Using multiple Snapshots
  //
  ////////////////////////////////////////////////////////

  // In this example, we set the snapshot multiple times.  This is probably
  // only necessary if you have very strict isolation requirements to
  // implement.

  // Set a snapshot at start of transaction
  txn_options.set_snapshot = true;
  txn = txn_db->BeginTransaction(write_options, txn_options);

  // Do some reads and writes to key "x"
  read_options.snapshot = db->GetSnapshot();
  s = txn->Get(read_options, "x", &value);
  assert(s.IsNotFound());
  s = txn->Put("x", "x");
  assert(s.ok());

  // The transaction hasn't committed, so the write is not visible
  // outside of txn.
  s = db->Get(read_options, "x", &value);
  assert(s.IsNotFound());

  // Do a write outside of the transaction to key "y"
  s = db->Put(write_options, "y", "z");
  assert(s.ok());

  // Set a new snapshot in the transaction
  txn->SetSnapshot();
  read_options.snapshot = db->GetSnapshot();

  // Do some reads and writes to key "y"
  s = txn->GetForUpdate(read_options, "y", &value);
  assert(s.ok());
  assert(value == "z");
  txn->Put("y", "y");

  // Commit.  Since the snapshot was advanced, the write done outside of the
  // transaction does not prevent this transaction from Committing.
  s = txn->Commit();
  assert(s.ok());
  delete txn;
  // Clear snapshot from read options since it is no longer valid
  read_options.snapshot = nullptr;

  // txn is committed, read the latest values.
  s = db->Get(read_options, "x", &value);
  assert(s.ok());
  assert(value == "x");

  s = db->Get(read_options, "y", &value);
  assert(s.ok());
  assert(value == "y");

  // print all values in the database
  ROCKSDB_NAMESPACE::Iterator* it =
      txn_db->NewIterator(ROCKSDB_NAMESPACE::ReadOptions());
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::cout << it->key().ToString() << ": " << it->value().ToString()
              << std::endl;
  }
  delete it;

  // Flush all data from main db to sst files. Release db.
  if (flushAtEnd) {
    txn_db->Flush(FlushOptions());
  }

  delete txn_db;

  fprintf(stdout, "Successfully used db at path %s in bucket %s.\n",
          kDBPath.c_str(), bucketName.c_str());
  return 0;
}
