//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-19, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "type/value_factory.h"

namespace bustub {

NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (plan->GetJoinType() != JoinType::LEFT && plan->GetJoinType() != JoinType::INNER) {
    // Note for 2022 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() { child_executor_->Init(); }

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    if (!right_rids_.empty()) {
      auto table_info = GetExecutorContext()->GetCatalog()->GetTable(plan_->GetInnerTableOid());
      table_info->table_->GetTuple(right_rids_.back(), &right_tuple_, GetExecutorContext()->GetTransaction());
      right_rids_.pop_back();

      std::vector<Value> values;
      values.reserve(GetOutputSchema().GetColumnCount());
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.push_back(left_tuple_.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); ++i) {
        values.push_back(right_tuple_.GetValue(&plan_->InnerTableSchema(), i));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      *rid = tuple->GetRid();
      return true;
    }

    if (!child_executor_->Next(&left_tuple_, &useless_)) {
      return false;
    }
    auto index_info = GetExecutorContext()->GetCatalog()->GetIndex(plan_->GetIndexName(), plan_->GetInnerTableOid());
    auto key = Tuple({plan_->KeyPredicate()->Evaluate(&left_tuple_, child_executor_->GetOutputSchema())},
                     &index_info->key_schema_);
    index_info->index_->ScanKey(key, &right_rids_, GetExecutorContext()->GetTransaction());
    if (right_rids_.empty() && plan_->GetJoinType() == JoinType::LEFT) {
      std::vector<Value> values;
      values.reserve(GetOutputSchema().GetColumnCount());
      for (uint32_t i = 0; i < child_executor_->GetOutputSchema().GetColumnCount(); ++i) {
        values.push_back(left_tuple_.GetValue(&child_executor_->GetOutputSchema(), i));
      }
      for (uint32_t i = 0; i < plan_->InnerTableSchema().GetColumnCount(); ++i) {
        values.push_back(ValueFactory::GetNullValueByType(plan_->InnerTableSchema().GetColumn(i).GetType()));
      }
      *tuple = Tuple(values, &GetOutputSchema());
      *rid = tuple->GetRid();
      return true;
    }
  }
}

}  // namespace bustub
