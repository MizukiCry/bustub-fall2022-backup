//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#include "execution/executors/index_scan_executor.h"

namespace bustub {
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      index_info_(GetExecutorContext()->GetCatalog()->GetIndex(plan->GetIndexOid())) {}

void IndexScanExecutor::Init() {
  auto *tree = dynamic_cast<BPlusTreeIndexForOneIntegerColumn *>(index_info_->index_.get());
  for (auto it = tree->GetBeginIterator(); !it.IsEnd(); ++it) {
    rids_.push_back((*it).second);
  }
  iter_ = rids_.begin();
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == rids_.end()) {
    return false;
  }

  auto txn = GetExecutorContext()->GetTransaction();
  *rid = *iter_++;
  GetExecutorContext()->GetCatalog()->GetTable(index_info_->table_name_)->table_->GetTuple(*rid, tuple, txn);
  return true;
}

}  // namespace bustub
