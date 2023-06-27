//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <iostream>
#include <sstream>

#include "common/config.h"
#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType {
  BUSTUB_ASSERT(0 <= index && index < GetMaxSize(), "index out of range.");
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  BUSTUB_ASSERT(0 <= index && index < GetMaxSize(), "index out of range.");
  array_[index].first = key;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType {
  BUSTUB_ASSERT(0 <= index && index < GetMaxSize(), "index out of range.");
  return array_[index].second;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) {
  BUSTUB_ASSERT(0 <= index && index < GetMaxSize(), "index out of range.");
  array_[index].second = value;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(int index, const KeyType &key, const ValueType &value) {
  const int size = GetSize();
  BUSTUB_ASSERT(0 <= index && index <= size, "index out of range.");
  BUSTUB_ASSERT(size != GetMaxSize(), "internal node is full.");
  std::copy_backward(array_ + index, array_ + size, array_ + size + 1);
  IncreaseSize(1);
  array_[index] = {key, value};
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Delete(int index) {
  BUSTUB_ASSERT(0 <= index && index <= GetSize(), "index out of range.");
  BUSTUB_ASSERT(GetSize() != 0, "internal node is empty.");
  std::copy(array_ + index + 1, array_ + GetSize(), array_ + index);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::ShiftFrom(B_PLUS_TREE_INTERNAL_PAGE_TYPE *other_page,
                                               BufferPoolManager *buffer_pool_manager) {
  page_id_t page_id = other_page->ValueAt(0);
  auto page = buffer_pool_manager->FetchPage(page_id);
  reinterpret_cast<BPlusTreePage *>(page->GetData())->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(page_id, true);
  Insert(GetSize(), other_page->KeyAt(0), page_id);
  other_page->Delete(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::ShiftTo(B_PLUS_TREE_INTERNAL_PAGE_TYPE *other_page,
                                             BufferPoolManager *buffer_pool_manager) {
  page_id_t page_id = ValueAt(GetSize() - 1);
  auto page = buffer_pool_manager->FetchPage(page_id);
  reinterpret_cast<BPlusTreePage *>(page->GetData())->SetParentPageId(other_page->GetPageId());
  buffer_pool_manager->UnpinPage(page_id, true);
  other_page->Insert(0, KeyAt(GetSize() - 1), page_id);
  Delete(GetSize() - 1);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Split(B_PLUS_TREE_INTERNAL_PAGE_TYPE *other_page, KeyType &end_key,
                                           ValueType &end_value) -> KeyType {
  int size = GetSize();
  std::copy(array_ + size / 2 + 1, array_ + size, other_page->array_);
  SetSize(size / 2 + 1);
  other_page->SetSize(size - size / 2 - 1);
  other_page->Insert(other_page->GetSize(), end_key, end_value);
  return other_page->KeyAt(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Merge(B_PLUS_TREE_INTERNAL_PAGE_TYPE *other_page, const KeyType &merge_key,
                                           BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < other_page->GetSize(); ++i) {
    auto page = buffer_pool_manager->FetchPage(other_page->ValueAt(i));
    // page->WLatch();
    reinterpret_cast<BPlusTreePage *>(page->GetData())->SetParentPageId(GetPageId());
    // page->WUnlatch();
    buffer_pool_manager->UnpinPage(other_page->ValueAt(i), true);
  }
  std::copy(other_page->array_, other_page->array_ + other_page->GetSize(), array_ + GetSize());
  SetKeyAt(GetSize(), merge_key);
  IncreaseSize(other_page->GetSize());
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
