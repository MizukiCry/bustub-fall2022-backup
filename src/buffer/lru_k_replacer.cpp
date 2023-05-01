//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include <iostream>
#include <mutex>  // NOLINT
#include "common/config.h"
#include "common/macros.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k)
    : replacer_size_(num_frames), k_(k), frames_(num_frames + 1, Frame(k_)) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  *frame_id = -1;

  static auto frame_cmp = [](const Frame &a, const Frame &b) -> bool {
    return a.Full() == b.Full() ? a.Timestamp() < b.Timestamp() : !a.Full();
  };

  std::scoped_lock<std::mutex> lock(latch_);

  for (size_t i = 0; i < replacer_size_; ++i) {
    auto &frame = frames_[i];
    if (!frame.evictable_) {
      continue;
    }
    if (*frame_id == -1 || frame_cmp(frames_[i], frames_[*frame_id])) {
      *frame_id = i;
    }
  }
  if (*frame_id == -1) {
    return false;
  }
  frames_[*frame_id].Reset();
  --curr_size_;
  return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id < static_cast<int>(replacer_size_), "frame id is invalid.");
  std::scoped_lock<std::mutex> lock(latch_);
  auto &frame = frames_[frame_id];
  if (!frame.valid_) {
    frame.valid_ = true;
    frame.evictable_ = true;
    ++curr_size_;
  }
  frame.Access(current_timestamp_++);
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(frame_id < static_cast<int>(replacer_size_), "frame id is invalid.");
  std::scoped_lock<std::mutex> lock(latch_);
  auto &frame = frames_[frame_id];
  // BUSTUB_ASSERT(frame.valid_, "frame id is invalid.");
  if (!frame.valid_) {
    return;
  }
  if (frame.evictable_ != set_evictable) {
    frame.evictable_ = set_evictable;
    curr_size_ += set_evictable ? 1 : -1;
  }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  if (frame_id >= static_cast<int>(replacer_size_)) {
    return;
  }
  std::scoped_lock<std::mutex> lock(latch_);
  auto &frame = frames_[frame_id];
  if (!frame.valid_) {
    return;
  }
  BUSTUB_ASSERT(frame.evictable_, "non-evictable frame is removed.");
  frame.Reset();
  --curr_size_;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
