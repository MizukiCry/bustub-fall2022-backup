#include "execution/plans/limit_plan.h"
#include "execution/plans/sort_plan.h"
#include "execution/plans/topn_plan.h"
#include "optimizer/optimizer.h"

namespace bustub {

auto Optimizer::OptimizeSortLimitAsTopN(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement sort + limit -> top N optimizer rule
  std::vector<AbstractPlanNodeRef> children;
  for (auto &child : plan->GetChildren()) {
    children.push_back(OptimizeSortLimitAsTopN(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  if (optimized_plan->GetType() == PlanType::Limit) {
    auto &limit_plan = dynamic_cast<const LimitPlanNode &>(*optimized_plan);
    if (optimized_plan->GetChildAt(0)->GetType() == PlanType::Sort) {
      auto &sort_plan = dynamic_cast<const SortPlanNode &>(*optimized_plan->GetChildAt(0));
      return std::make_shared<TopNPlanNode>(limit_plan.output_schema_, sort_plan.GetChildAt(0), sort_plan.GetOrderBy(),
                                            limit_plan.GetLimit());
    }
  }
  return optimized_plan;
}

}  // namespace bustub
