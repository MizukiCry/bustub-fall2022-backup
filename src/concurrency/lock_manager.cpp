//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include "common/config.h"
#include "concurrency/transaction.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

[[noreturn]] void LockManager::AbortTransaction(Transaction *txn, AbortReason abort_reason) {
  txn->SetState(TransactionState::ABORTED);
  throw TransactionAbortException(txn->GetTransactionId(), abort_reason);
}

void LockManager::UpdateTableLockSet(Transaction *txn, LockMode lock_mode, table_oid_t oid, bool insert) {
  std::shared_ptr<std::unordered_set<table_oid_t>> lock_set;
  switch (lock_mode) {
    case LockMode::SHARED:
      lock_set = txn->GetSharedTableLockSet();
      break;
    case LockMode::EXCLUSIVE:
      lock_set = txn->GetExclusiveTableLockSet();
      break;
    case LockMode::INTENTION_SHARED:
      lock_set = txn->GetIntentionSharedTableLockSet();
      break;
    case LockMode::INTENTION_EXCLUSIVE:
      lock_set = txn->GetIntentionExclusiveTableLockSet();
      break;
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      lock_set = txn->GetSharedIntentionExclusiveTableLockSet();
      break;
  }
  if (insert) {
    lock_set->insert(oid);
  } else {
    lock_set->erase(oid);
  }
}

void LockManager::UpdateRowLockSet(Transaction *txn, const std::shared_ptr<LockRequest> &lock_request, bool insert) {
  switch (lock_request->lock_mode_) {
    case LockMode::INTENTION_SHARED:
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      return;
    default:
      auto lock_set =
          lock_request->lock_mode_ == LockMode::SHARED ? txn->GetSharedRowLockSet() : txn->GetExclusiveRowLockSet();
      if (insert) {
        (*lock_set)[lock_request->oid_].insert(lock_request->rid_);
      } else {
        (*lock_set)[lock_request->oid_].erase(lock_request->rid_);
      }
  }
}

auto LockManager::GrantLock(const std::shared_ptr<LockRequest> &lock_request,
                            const std::shared_ptr<LockRequestQueue> &lock_request_queue) -> bool {
  for (auto &request : lock_request_queue->request_queue_) {
    if (request.get() == lock_request.get()) {
      break;
    }
    if (!request->granted_) {
      return false;
    }
    switch (lock_request->lock_mode_) {
      case LockMode::SHARED:
        if (request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE ||
            request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE || request->lock_mode_ == LockMode::EXCLUSIVE) {
          return false;
        }
        break;
      case LockMode::EXCLUSIVE:
        return false;
        break;
      case LockMode::INTENTION_SHARED:
        if (request->lock_mode_ == LockMode::EXCLUSIVE) {
          return false;
        }
        break;
      case LockMode::INTENTION_EXCLUSIVE:
        if (request->lock_mode_ == LockMode::SHARED || request->lock_mode_ == LockMode::SHARED_INTENTION_EXCLUSIVE ||
            request->lock_mode_ == LockMode::EXCLUSIVE) {
          return false;
        }
        break;
      case LockMode::SHARED_INTENTION_EXCLUSIVE:
        if (request->lock_mode_ != LockMode::INTENTION_SHARED) {
          return false;
        }
        break;
    }
  }
  return true;
}

auto LockManager::LockTable(Transaction *txn, LockMode lock_mode, const table_oid_t &oid) -> bool {
  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      if (lock_mode == LockMode::SHARED || lock_mode == LockMode::INTENTION_SHARED ||
          lock_mode == LockMode::SHARED_INTENTION_EXCLUSIVE) {
        AbortTransaction(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      if (txn->GetState() == TransactionState::SHRINKING &&
          (lock_mode == LockMode::EXCLUSIVE || lock_mode == LockMode::INTENTION_EXCLUSIVE)) {
        AbortTransaction(txn, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::INTENTION_SHARED &&
          lock_mode != LockMode::SHARED) {
        AbortTransaction(txn, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::REPEATABLE_READ:
      if (txn->GetState() == TransactionState::SHRINKING) {
        AbortTransaction(txn, AbortReason::LOCK_ON_SHRINKING);
      }
  }

  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_[oid] = std::make_shared<LockRequestQueue>();
  }
  auto lock_request_queue = table_lock_map_[oid];
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();

  for (auto lock_request : lock_request_queue->request_queue_) {  // NOLINT
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      if (lock_request->lock_mode_ == lock_mode) {
        lock_request_queue->latch_.unlock();
        return true;
      }

      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        lock_request_queue->latch_.unlock();
        AbortTransaction(txn, AbortReason::UPGRADE_CONFLICT);
      }

      if ((lock_request->lock_mode_ != LockMode::INTENTION_SHARED ||
           (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE &&
            lock_mode != LockMode::INTENTION_EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          (lock_request->lock_mode_ != LockMode::SHARED ||
           (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          (lock_request->lock_mode_ != LockMode::INTENTION_EXCLUSIVE ||
           (lock_mode != LockMode::EXCLUSIVE && lock_mode != LockMode::SHARED_INTENTION_EXCLUSIVE)) &&
          (lock_request->lock_mode_ != LockMode::SHARED_INTENTION_EXCLUSIVE || (lock_mode != LockMode::EXCLUSIVE))) {
        lock_request_queue->latch_.unlock();
        AbortTransaction(txn, AbortReason::INCOMPATIBLE_UPGRADE);
      }

      lock_request_queue->request_queue_.remove(lock_request);
      UpdateTableLockSet(txn, lock_request->lock_mode_, lock_request->oid_, false);
      auto new_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);

      auto position = std::find_if(lock_request_queue->request_queue_.begin(), lock_request_queue->request_queue_.end(),
                                   [](auto i) { return !(*i).granted_; });
      lock_request_queue->request_queue_.insert(position, new_lock_request);
      lock_request_queue->upgrading_ = txn->GetTransactionId();

      std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
      while (!GrantLock(new_lock_request, lock_request_queue)) {
        lock_request_queue->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock_request_queue->request_queue_.remove(new_lock_request);
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }

      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      new_lock_request->granted_ = true;
      UpdateTableLockSet(txn, new_lock_request->lock_mode_, new_lock_request->oid_, true);
      if (lock_mode != LockMode::EXCLUSIVE) {
        lock_request_queue->cv_.notify_all();
      }
      return true;
    }
  }

  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid);
  lock_request_queue->request_queue_.push_back(lock_request);
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!GrantLock(lock_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  lock_request->granted_ = true;
  UpdateTableLockSet(txn, lock_request->lock_mode_, lock_request->oid_, true);
  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockTable(Transaction *txn, const table_oid_t &oid) -> bool {
  table_lock_map_latch_.lock();
  if (table_lock_map_.find(oid) == table_lock_map_.end()) {
    table_lock_map_latch_.unlock();
    AbortTransaction(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  if ((txn->GetSharedRowLockSet()->find(oid) != txn->GetSharedRowLockSet()->end() &&
       !(*txn->GetSharedRowLockSet())[oid].empty()) ||
      (txn->GetExclusiveRowLockSet()->find(oid) != txn->GetExclusiveRowLockSet()->end() &&
       !(*txn->GetExclusiveRowLockSet())[oid].empty())) {
    table_lock_map_latch_.unlock();
    AbortTransaction(txn, AbortReason::TABLE_UNLOCKED_BEFORE_UNLOCKING_ROWS);
  }
  auto lock_request_queue = table_lock_map_[oid];
  lock_request_queue->latch_.lock();
  table_lock_map_latch_.unlock();

  for (auto &lock_request : lock_request_queue->request_queue_) {
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      auto lock_mode = lock_request->lock_mode_;
      auto oid = lock_request->oid_;
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      lock_request_queue->latch_.unlock();
      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           (lock_mode == LockMode::SHARED || lock_mode == LockMode::EXCLUSIVE)) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mode == LockMode::EXCLUSIVE) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED && lock_mode == LockMode::EXCLUSIVE)) {
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
      UpdateTableLockSet(txn, lock_mode, oid, false);
      return true;
    }
  }

  lock_request_queue->latch_.unlock();
  AbortTransaction(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

auto LockManager::LockRow(Transaction *txn, LockMode lock_mode, const table_oid_t &oid, const RID &rid) -> bool {
  switch (lock_mode) {
    case LockMode::INTENTION_EXCLUSIVE:
    case LockMode::INTENTION_SHARED:
    case LockMode::SHARED_INTENTION_EXCLUSIVE:
      AbortTransaction(txn, AbortReason::ATTEMPTED_INTENTION_LOCK_ON_ROW);
      break;
    case LockMode::EXCLUSIVE:
      if (!txn->IsTableExclusiveLocked(oid) && !txn->IsTableIntentionExclusiveLocked(oid) &&
          !txn->IsTableSharedIntentionExclusiveLocked(oid)) {
        AbortTransaction(txn, AbortReason::TABLE_LOCK_NOT_PRESENT);
      }
    case LockMode::SHARED:
      break;
  }

  switch (txn->GetIsolationLevel()) {
    case IsolationLevel::READ_UNCOMMITTED:
      if (lock_mode == LockMode::SHARED) {
        AbortTransaction(txn, AbortReason::LOCK_SHARED_ON_READ_UNCOMMITTED);
      }
      if (txn->GetState() == TransactionState::SHRINKING && lock_mode == LockMode::EXCLUSIVE) {
        AbortTransaction(txn, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::READ_COMMITTED:
      if (txn->GetState() == TransactionState::SHRINKING && lock_mode != LockMode::SHARED) {
        AbortTransaction(txn, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
    case IsolationLevel::REPEATABLE_READ:
      if (txn->GetState() == TransactionState::SHRINKING) {
        AbortTransaction(txn, AbortReason::LOCK_ON_SHRINKING);
      }
      break;
  }

  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_.emplace(rid, std::make_shared<LockRequestQueue>());
  }
  auto lock_request_queue = row_lock_map_[rid];
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();

  for (auto lock_request : lock_request_queue->request_queue_) {  // NOLINT
    if (lock_request->txn_id_ == txn->GetTransactionId()) {
      if (lock_request->lock_mode_ == lock_mode) {
        lock_request_queue->latch_.unlock();
        return true;
      }

      if (lock_request_queue->upgrading_ != INVALID_TXN_ID) {
        lock_request_queue->latch_.unlock();
        AbortTransaction(txn, AbortReason::UPGRADE_CONFLICT);
      }

      if ((lock_request->lock_mode_ != LockMode::INTENTION_SHARED ||
           (lock_mode != LockMode::SHARED && lock_mode != LockMode::EXCLUSIVE)) &&
          (lock_request->lock_mode_ != LockMode::SHARED || lock_mode != LockMode::EXCLUSIVE) &&
          (lock_request->lock_mode_ != LockMode::INTENTION_EXCLUSIVE || lock_mode != LockMode::EXCLUSIVE) &&
          (lock_request->lock_mode_ != LockMode::SHARED_INTENTION_EXCLUSIVE || lock_mode != LockMode::EXCLUSIVE)) {
        lock_request_queue->latch_.unlock();
        AbortTransaction(txn, AbortReason::INCOMPATIBLE_UPGRADE);
      }

      lock_request_queue->request_queue_.remove(lock_request);
      UpdateRowLockSet(txn, lock_request, false);
      auto new_lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);

      auto position = std::find_if(lock_request_queue->request_queue_.begin(), lock_request_queue->request_queue_.end(),
                                   [](auto i) { return !(*i).granted_; });
      lock_request_queue->request_queue_.insert(position, new_lock_request);
      lock_request_queue->upgrading_ = txn->GetTransactionId();

      std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
      while (!GrantLock(new_lock_request, lock_request_queue)) {
        lock_request_queue->cv_.wait(lock);
        if (txn->GetState() == TransactionState::ABORTED) {
          lock_request_queue->upgrading_ = INVALID_TXN_ID;
          lock_request_queue->request_queue_.remove(new_lock_request);
          lock_request_queue->cv_.notify_all();
          return false;
        }
      }

      lock_request_queue->upgrading_ = INVALID_TXN_ID;
      new_lock_request->granted_ = true;
      UpdateRowLockSet(txn, new_lock_request, true);
      if (lock_mode != LockMode::EXCLUSIVE) {
        lock_request_queue->cv_.notify_all();
      }
      return true;
    }
  }

  auto lock_request = std::make_shared<LockRequest>(txn->GetTransactionId(), lock_mode, oid, rid);
  lock_request_queue->request_queue_.push_back(lock_request);
  std::unique_lock<std::mutex> lock(lock_request_queue->latch_, std::adopt_lock);
  while (!GrantLock(lock_request, lock_request_queue)) {
    lock_request_queue->cv_.wait(lock);
    if (txn->GetState() == TransactionState::ABORTED) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      return false;
    }
  }
  lock_request->granted_ = true;
  UpdateRowLockSet(txn, lock_request, true);
  if (lock_mode != LockMode::EXCLUSIVE) {
    lock_request_queue->cv_.notify_all();
  }
  return true;
}

auto LockManager::UnlockRow(Transaction *txn, const table_oid_t &oid, const RID &rid) -> bool {
  row_lock_map_latch_.lock();
  if (row_lock_map_.find(rid) == row_lock_map_.end()) {
    row_lock_map_latch_.unlock();
    AbortTransaction(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
  }
  auto lock_request_queue = row_lock_map_[rid];
  lock_request_queue->latch_.lock();
  row_lock_map_latch_.unlock();

  for (auto lock_request : lock_request_queue->request_queue_) {  // NOLINT
    if (lock_request->txn_id_ == txn->GetTransactionId() && lock_request->granted_) {
      lock_request_queue->request_queue_.remove(lock_request);
      lock_request_queue->cv_.notify_all();
      lock_request_queue->latch_.unlock();
      if ((txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ &&
           (lock_request->lock_mode_ == LockMode::SHARED || lock_request->lock_mode_ == LockMode::EXCLUSIVE)) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_COMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE) ||
          (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED &&
           lock_request->lock_mode_ == LockMode::EXCLUSIVE)) {
        if (txn->GetState() != TransactionState::COMMITTED && txn->GetState() != TransactionState::ABORTED) {
          txn->SetState(TransactionState::SHRINKING);
        }
      }
      UpdateRowLockSet(txn, lock_request, false);
      return true;
    }
  }
  lock_request_queue->latch_.unlock();
  AbortTransaction(txn, AbortReason::ATTEMPTED_UNLOCK_BUT_NO_LOCK_HELD);
}

void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) {
  waits_for_[t1].push_back(t2);
  txn_set_.insert(t1);
  txn_set_.insert(t2);
}

void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) {
  auto p = std::find(waits_for_[t1].begin(), waits_for_[t1].end(), t2);
  if (p != waits_for_[t1].end()) {
    waits_for_[t1].erase(p);
  }
}

auto LockManager::HasCycle(txn_id_t *txn_id) -> bool {
  return std::any_of(txn_set_.begin(), txn_set_.end(), [this, txn_id](txn_id_t txn) {
    bool res = FindCycle(txn);
    if (res) {
      *txn_id = *std::max_element(active_set_.begin(), active_set_.end());
    }
    active_set_.clear();
    return res;
  });
}

auto LockManager::GetEdgeList() -> std::vector<std::pair<txn_id_t, txn_id_t>> {
  std::vector<std::pair<txn_id_t, txn_id_t>> edges(0);
  for (auto &pair : waits_for_) {
    for (auto t2 : pair.second) {
      edges.emplace_back(pair.first, t2);
    }
  }
  return edges;
}

void LockManager::RunCycleDetection() {
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      table_lock_map_latch_.lock();
      row_lock_map_latch_.lock();

      for (auto &[_, lock_request_queue] : table_lock_map_) {
        std::unordered_set<txn_id_t> granted_set;
        std::lock_guard<std::mutex> guard(lock_request_queue->latch_);
        for (auto &lock_request : lock_request_queue->request_queue_) {
          if (lock_request->granted_) {
            granted_set.insert(lock_request->txn_id_);
          } else {
            for (auto txn_id : granted_set) {
              waits_for_oid_[lock_request->txn_id_] = lock_request->oid_;
              AddEdge(lock_request->txn_id_, txn_id);
            }
          }
        }
      }

      for (auto &[_, lock_request_queue] : row_lock_map_) {
        std::unordered_set<txn_id_t> granted_set;
        std::lock_guard<std::mutex> guard(lock_request_queue->latch_);
        for (auto &lock_request : lock_request_queue->request_queue_) {
          if (lock_request->granted_) {
            granted_set.insert(lock_request->txn_id_);
          } else {
            for (auto txn_id : granted_set) {
              waits_for_rid_[lock_request->txn_id_] = lock_request->rid_;
              AddEdge(lock_request->txn_id_, txn_id);
            }
          }
        }
      }

      row_lock_map_latch_.unlock();
      table_lock_map_latch_.unlock();

      for (txn_id_t txn_id; HasCycle(&txn_id);) {
        TransactionManager::GetTransaction(txn_id)->SetState(TransactionState::ABORTED);
        waits_for_.erase(txn_id);
        for (auto next_txn_id : txn_set_) {
          RemoveEdge(next_txn_id, txn_id);
        }

        if (waits_for_oid_.find(txn_id) != waits_for_oid_.end()) {
          table_lock_map_[waits_for_oid_[txn_id]]->latch_.lock();
          table_lock_map_[waits_for_oid_[txn_id]]->cv_.notify_all();
          table_lock_map_[waits_for_oid_[txn_id]]->latch_.unlock();
        }

        if (waits_for_rid_.find(txn_id) != waits_for_rid_.end()) {
          row_lock_map_[waits_for_rid_[txn_id]]->latch_.lock();
          row_lock_map_[waits_for_rid_[txn_id]]->cv_.notify_all();
          row_lock_map_[waits_for_rid_[txn_id]]->latch_.unlock();
        }
      }

      waits_for_.clear();
      safe_set_.clear();
      txn_set_.clear();
      waits_for_oid_.clear();
      waits_for_rid_.clear();
    }
  }
}

auto LockManager::FindCycle(txn_id_t txn_id) -> bool {
  if (safe_set_.find(txn_id) != safe_set_.end()) {
    return false;
  }
  if (active_set_.find(txn_id) != active_set_.end()) {
    return true;
  }
  active_set_.insert(txn_id);
  std::vector<txn_id_t> g = waits_for_[txn_id];
  std::sort(g.begin(), g.end());
  for (auto next_txn_id : g) {
    if (FindCycle(next_txn_id)) {
      return true;
    }
  }
  active_set_.erase(txn_id);
  safe_set_.insert(txn_id);
  return false;
}

}  // namespace bustub
