/**
 * index_iterator.cpp
 */
#include <cassert>

#include "common/config.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, B_PLUS_TREE_LEAF_PAGE_TYPE *leaf, int pos)
    : buffer_pool_manager_(buffer_pool_manager), leaf_(leaf), pos_(pos) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (leaf_ != nullptr) {
    buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool { return leaf_ == nullptr; }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & { return leaf_->Ref(pos_); }

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  if (leaf_ != nullptr) {
    if (pos_ + 1 < leaf_->GetSize()) {
      ++pos_;
    } else {
      int next_page_id = leaf_->GetNextPageId();
      buffer_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
      if (next_page_id != INVALID_PAGE_ID) {
        leaf_ =
            reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(buffer_pool_manager_->FetchPage(next_page_id)->GetData());
      } else {
        leaf_ = nullptr;
      }
      pos_ = 0;
    }
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator<KeyType, ValueType, KeyComparator> &itr) const -> bool {
  return leaf_ == itr.leaf_ && pos_ == itr.pos_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator<KeyType, ValueType, KeyComparator> &itr) const -> bool {
  return !(*this == itr);
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
