//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), iter_(nullptr, RID(), nullptr) {}

void SeqScanExecutor::Init() {
  auto ctx = GetExecutorContext();
  auto txn = ctx->GetTransaction();
  if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    if (!ctx->GetLockManager()->LockTable(txn, LockManager::LockMode::INTENTION_SHARED,
                                          ctx->GetCatalog()->GetTable(plan_->GetTableOid())->oid_)) {
      throw ExecutionException("SeqScan Executor: failed to lock table");
    }
  }

  iter_ = ctx->GetCatalog()->GetTable(plan_->GetTableOid())->table_->Begin(txn);
}

auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto ctx = GetExecutorContext();
  auto txn = ctx->GetTransaction();
  auto oid = ctx->GetCatalog()->GetTable(plan_->GetTableOid())->oid_;
  if (iter_ == ctx->GetCatalog()->GetTable(plan_->GetTableOid())->table_->End()) {
    if (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED) {
      auto lock_set = (*txn->GetSharedRowLockSet())[oid];
      for (auto lrid : lock_set) {
        ctx->GetLockManager()->UnlockRow(txn, oid, lrid);
      }
      ctx->GetLockManager()->UnlockTable(txn, oid);
    }
    return false;
  }
  *tuple = *iter_++;
  *rid = tuple->GetRid();

  if (txn->GetIsolationLevel() != IsolationLevel::READ_UNCOMMITTED) {
    if (!ctx->GetLockManager()->LockRow(txn, LockManager::LockMode::SHARED, oid, *rid)) {
      throw ExecutionException("SeqScan Executor: failed to lock row");
    }
  }
  return true;
}

}  // namespace bustub
