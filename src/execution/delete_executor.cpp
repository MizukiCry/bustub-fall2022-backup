//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  child_executor_->Init();
  deleted_ = false;
  auto ctx = GetExecutorContext();
  if (!ctx->GetLockManager()->LockTable(ctx->GetTransaction(), LockManager::LockMode::INTENTION_EXCLUSIVE,
                                        ctx->GetCatalog()->GetTable(plan_->TableOid())->oid_)) {
    throw ExecutionException("Delete Executor: failed to lock table");
  }
}

auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (deleted_) {
    return false;
  }
  deleted_ = true;

  Tuple child_tuple;
  auto *ctx = GetExecutorContext();
  auto *txn = ctx->GetTransaction();
  auto *table_info = ctx->GetCatalog()->GetTable(plan_->TableOid());

  int count = 0;
  while (child_executor_->Next(&child_tuple, rid)) {
    if (!ctx->GetLockManager()->LockRow(txn, LockManager::LockMode::EXCLUSIVE, table_info->oid_, *rid)) {
      throw ExecutionException("Delete Executor: failed to lock row");
    }
    if (table_info->table_->MarkDelete(*rid, txn)) {
      ++count;
      for (auto index : ctx->GetCatalog()->GetTableIndexes(table_info->name_)) {
        index->index_->DeleteEntry(
            child_tuple.KeyFromTuple(table_info->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()),
            *rid, txn);
      }
    }
  }

  std::vector<Value> values;
  values.emplace_back(INTEGER, count);
  *tuple = Tuple(values, &GetOutputSchema());
  return true;
}

}  // namespace bustub
