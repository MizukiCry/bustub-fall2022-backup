#include "execution/executors/topn_executor.h"

namespace bustub {

TopNExecutor::TopNExecutor(ExecutorContext *exec_ctx, const TopNPlanNode *plan,
                           std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void TopNExecutor::Init() {
  child_executor_->Init();
  tuples_.clear();

  auto cmp = [order_bys = plan_->GetOrderBy(), schema = child_executor_->GetOutputSchema()](const Tuple &a,
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
  };

  Tuple tuple;
  RID rid;

  // while (child_executor_->Next(&tuple, &rid)) {
  //   tuples_.push_back(tuple);
  // }
  // std::nth_element(tuples_.begin(), tuples_.begin() + plan_->GetN() - 1, tuples_.end(), cmp);
  // tuples_.resize(plan_->GetN());
  // std::sort(tuples_.begin(), tuples_.end(), cmp);

  std::priority_queue<Tuple, std::vector<Tuple>, decltype(cmp)> q(cmp);
  while (child_executor_->Next(&tuple, &rid)) {
    q.push(tuple);
    if (q.size() > plan_->GetN()) {
      q.pop();
    }
  }
  while (!q.empty()) {
    tuples_.push_back(q.top());
    q.pop();
  }
  std::reverse(tuples_.begin(), tuples_.end());

  iter_ = tuples_.begin();
}

auto TopNExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  if (iter_ == tuples_.end()) {
    return false;
  }
  *tuple = *iter_++;
  *rid = tuple->GetRid();
  return true;
}

}  // namespace bustub
