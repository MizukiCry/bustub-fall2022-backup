//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  child_executor_->Init();
  first_ = true;
}

auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool {
  if (!first_) {
    return false;
  }
  first_ = false;

  auto *ctx = GetExecutorContext();
  auto *txn = ctx->GetTransaction();
  auto *table_info = ctx->GetCatalog()->GetTable(plan_->TableOid());
  Tuple child_tuple;

  int count = 0;
  while (child_executor_->Next(&child_tuple, rid)) {
    if (table_info->table_->InsertTuple(child_tuple, rid, txn)) {
      ++count;
      for (auto index : ctx->GetCatalog()->GetTableIndexes(table_info->name_)) {
        index->index_->InsertEntry(
            child_tuple.KeyFromTuple(table_info->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()),
            *rid, txn);
      }
    }
  }

  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, count);
  *tuple = Tuple{values, &GetOutputSchema()};
  return true;
}

}  // namespace bustub
