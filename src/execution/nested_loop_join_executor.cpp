//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestedLoopJoinExecutor::Init() {
  left_executor_->Init();
  right_executor_->Init();
  left_remains_ = left_executor_->Next(&left_tuple_, &useless_);
  left_matched_ = false;
}

auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (!left_remains_) {
      return false;
    }

    if (!right_executor_->Next(&right_tuple_, &useless_)) {
      if (plan_->GetJoinType() == JoinType::LEFT && !left_matched_) {
        std::vector<Value> values;
        values.reserve(GetOutputSchema().GetColumnCount());
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
          values.push_back(ValueFactory::GetNullValueByType(right_executor_->GetOutputSchema().GetColumn(i).GetType()));
        }
        *tuple = Tuple(values, &GetOutputSchema());
        *rid = tuple->GetRid();
        left_matched_ = true;
        return true;
      }
      right_executor_->Init();
      left_remains_ = left_executor_->Next(&left_tuple_, &useless_);
      left_matched_ = false;
      continue;
    }

    auto res = plan_->Predicate().EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(), &right_tuple_,
                                               right_executor_->GetOutputSchema());
    if (!res.IsNull() && res.GetAs<bool>()) {
      std::vector<Value> values;
      values.reserve(GetOutputSchema().GetColumnCount());
      for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.push_back(right_tuple_.GetValue(&right_executor_->GetOutputSchema(), i));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      *rid = tuple->GetRid();
      left_matched_ = true;
      return true;
    }
  }
}

}  // namespace bustub
