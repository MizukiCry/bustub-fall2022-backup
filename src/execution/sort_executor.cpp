#include "execution/executors/sort_executor.h"

namespace bustub {

SortExecutor::SortExecutor(ExecutorContext *exec_ctx, const SortPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void SortExecutor::Init() {
  child_executor_->Init();
  tuples_.clear();
  Tuple tuple;
  RID rid;
  while (child_executor_->Next(&tuple, &rid)) {
    tuples_.push_back(tuple);
  }
  std::sort(tuples_.begin(), tuples_.end(),
            [order_bys = plan_->GetOrderBy(), schema = child_executor_->GetOutputSchema()](const Tuple &a,
                                                                                           const Tuple &b) -> bool {
              for (auto &order_key : order_bys) {
                auto va = order_key.second->Evaluate(&a, schema);
                auto vb = order_key.second->Evaluate(&b, schema);
                if (va.CompareEquals(vb) != CmpBool::CmpTrue) {
                  if (order_key.first == OrderByType::DESC) {
                    return va.CompareGreaterThan(vb) == CmpBool::CmpTrue;
                  }
                  return va.CompareLessThan(vb) == CmpBool::CmpTrue;
                }
              }
              return false;
            });
  iter_ = tuples_.begin();
}

auto SortExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == tuples_.end()) {
    return false;
  }
  *tuple = *iter_++;
  *rid = tuple->GetRid();
  return true;
}

}  // namespace bustub
