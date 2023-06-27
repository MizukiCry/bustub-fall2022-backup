#include <cassert>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <mutex>  // NOLINT
#include <stack>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "storage/index/b_plus_tree.h"
#include "storage/index/index_iterator.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {
  BUSTUB_ASSERT(leaf_max_size >= 2, "leaf_max_size too small");
  BUSTUB_ASSERT(internal_max_size >= 3, "internal_max_size too small");
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetPosOnInternalPage(InternalPage *page, const KeyType &key) const -> int {
  int l = 1;
  int r = page->GetSize();
  while (l < r) {
    if (int mid = (l + r) / 2; comparator_(page->KeyAt(mid), key) != 1) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }
  return l - 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetPosOnLeafPage(LeafPage *page, const KeyType &key) const -> int {
  int l = 0;
  int r = page->GetSize();
  while (l < r) {
    if (int mid = (l + r) / 2; comparator_(page->KeyAt(mid), key) != 1) {
      l = mid + 1;
    } else {
      r = mid;
    }
  }
  return l - 1;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::FindLeaf(const KeyType &key, FindType type, Transaction *transaction, const BoundaryType boundary)
    -> Page * {
  auto page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
  if (type == FindType::kSearch) {
    page->RLatch();
    root_latch_.RUnlock();
  } else {
    page->WLatch();
    if (type == FindType::kDelete ? node->GetSize() > 2
                                  : node->GetSize() + (node->IsLeafPage() ? 1 : 0) < node->GetMaxSize()) {
      ReleaseQueueLatch(transaction);
    }
  }

  while (!node->IsLeafPage()) {
    auto inode = reinterpret_cast<InternalPage *>(node);
    auto child_page_id = inode->ValueAt(boundary == BoundaryType::kNormal
                                            ? GetPosOnInternalPage(inode, key)
                                            : (boundary == BoundaryType::kLeftMost ? 0 : inode->GetSize() - 1));
    auto child_page = buffer_pool_manager_->FetchPage(child_page_id);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    if (type == FindType::kSearch) {
      child_page->RLatch();
      page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else {
      child_page->WLatch();
      transaction->AddIntoPageSet(page);
      if (type == FindType::kDelete
              ? child_node->GetSize() > child_node->GetMinSize()
              : child_node->GetSize() + (child_node->IsLeafPage() ? 1 : 0) < child_node->GetMaxSize()) {
        ReleaseQueueLatch(transaction);
      }
    }
    page = child_page;
    node = child_node;
  }
  return page;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  root_latch_.RLock();
  auto leaf_page = FindLeaf(key, FindType::kSearch, transaction);
  auto leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int pos = GetPosOnLeafPage(leaf, key);
  result->clear();
  if (pos != -1 && comparator_(key, leaf->KeyAt(pos)) == 0) {
    result->push_back(leaf->ValueAt(pos));
  }
  leaf_page->RUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  // root_latch_.RUnlock();
  return !result->empty();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReleaseQueueLatch(Transaction *transaction) {
  while (!transaction->GetPageSet()->empty()) {
    auto page = transaction->GetPageSet()->front();
    transaction->GetPageSet()->pop_front();
    if (page == nullptr) {
      root_latch_.WUnlock();
    } else {
      page->WUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (node->IsRootPage()) {
    auto page = buffer_pool_manager_->NewPage(&root_page_id_);
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Failed to allocate new page");
    }

    auto new_root = reinterpret_cast<InternalPage *>(page->GetData());
    new_root->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    new_root->Insert(0,
                     node->IsLeafPage() ? reinterpret_cast<LeafPage *>(node)->KeyAt(0)
                                        : reinterpret_cast<InternalPage *>(node)->KeyAt(0),
                     node->GetPageId());
    new_root->Insert(1, key, new_node->GetPageId());
    node->SetParentPageId(root_page_id_);
    new_node->SetParentPageId(root_page_id_);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    UpdateRootPageId();
    ReleaseQueueLatch(transaction);
    return;
  }
  auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto parent_node = reinterpret_cast<InternalPage *>(parent_page->GetData());
  if (parent_node->GetSize() < parent_node->GetMaxSize()) {
    parent_node->Insert(GetPosOnInternalPage(parent_node, key) + 1, key, new_node->GetPageId());
    ReleaseQueueLatch(transaction);
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return;
  }

  page_id_t new_parent_page_id;
  auto new_parent_page = buffer_pool_manager_->NewPage(&new_parent_page_id);
  if (new_parent_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Failed to allocate new page");
  }
  auto new_parent = reinterpret_cast<InternalPage *>(new_parent_page->GetData());
  new_parent->Init(new_parent_page->GetPageId(), parent_node->GetParentPageId(), internal_max_size_);
  KeyType end_key = key;
  page_id_t end_value = new_node->GetPageId();
  if (comparator_(key, parent_node->KeyAt(parent_node->GetSize() - 1)) == -1) {
    end_key = parent_node->KeyAt(parent_node->GetSize() - 1);
    end_value = parent_node->ValueAt(parent_node->GetSize() - 1);
    parent_node->Delete(parent_node->GetSize() - 1);
    parent_node->Insert(GetPosOnInternalPage(parent_node, key) + 1, key, new_node->GetPageId());
  }
  parent_node->Split(new_parent, end_key, end_value);
  for (int i = 0; i < new_parent->GetSize(); ++i) {
    auto child_page = buffer_pool_manager_->FetchPage(new_parent->ValueAt(i));
    reinterpret_cast<BPlusTreePage *>(child_page->GetData())->SetParentPageId(new_parent->GetPageId());
    buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
  }
  InsertIntoParent(parent_node, new_parent->KeyAt(0), new_parent, transaction);
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_parent_page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  root_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);
  if (IsEmpty()) {
    auto page = buffer_pool_manager_->NewPage(&root_page_id_);
    if (page == nullptr) {
      throw Exception(ExceptionType::OUT_OF_MEMORY, "Failed to allocate new page");
    }
    auto leaf = reinterpret_cast<LeafPage *>(page->GetData());
    leaf->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);
    leaf->Insert(0, key, value);
    buffer_pool_manager_->UnpinPage(root_page_id_, true);
    ReleaseQueueLatch(transaction);
    return true;
  }

  auto leaf_page = FindLeaf(key, FindType::kInsert, transaction);
  auto leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int pos = GetPosOnLeafPage(leaf, key);
  if (pos != -1 && comparator_(key, leaf->KeyAt(pos)) == 0) {
    ReleaseQueueLatch(transaction);
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return false;
  }

  leaf->Insert(pos + 1, key, value);
  if (leaf->GetSize() != leaf->GetMaxSize()) {
    ReleaseQueueLatch(transaction);
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    return true;
  }

  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "Failed to allocate new page");
  }
  auto new_leaf = reinterpret_cast<LeafPage *>(new_page->GetData());
  new_leaf->Init(new_page->GetPageId(), leaf->GetParentPageId(), leaf_max_size_);
  leaf->Split(new_leaf);

  InsertIntoParent(leaf, new_leaf->KeyAt(0), new_leaf, transaction);

  leaf_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_leaf->GetPageId(), true);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename NodeType>
auto BPLUSTREE_TYPE::CoalesceOrRedistribute(NodeType *node, Transaction *transaction) -> bool {
  if (node->IsRootPage()) {
    if (!node->IsLeafPage() && node->GetSize() == 1) {
      auto root = reinterpret_cast<InternalPage *>(node);
      auto child_page = buffer_pool_manager_->FetchPage(root->ValueAt(0));
      auto child = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
      child->SetParentPageId(INVALID_PAGE_ID);
      root_page_id_ = child->GetPageId();
      UpdateRootPageId();
      buffer_pool_manager_->UnpinPage(root_page_id_, true);
      ReleaseQueueLatch(transaction);
      return true;
    }
    if (node->IsLeafPage() && node->GetSize() == 0) {
      root_page_id_ = INVALID_PAGE_ID;
      ReleaseQueueLatch(transaction);
      return true;
    }
    ReleaseQueueLatch(transaction);
    return false;
  }
  if (node->GetSize() >= node->GetMinSize()) {
    ReleaseQueueLatch(transaction);
    return false;
  }

  auto parent_page = buffer_pool_manager_->FetchPage(node->GetParentPageId());
  auto parent = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int pos = GetPosOnInternalPage(parent, node->KeyAt(0));

  if (pos > 0) {
    auto neighbor_page = buffer_pool_manager_->FetchPage(parent->ValueAt(pos - 1));
    neighbor_page->WLatch();
    auto neighbor = reinterpret_cast<NodeType *>(neighbor_page->GetData());
    if (neighbor->GetSize() > neighbor->GetMinSize()) {
      Redistribute(neighbor, node, parent, pos, true);
      ReleaseQueueLatch(transaction);
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      neighbor_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(neighbor->GetPageId(), true);
      return false;
    }
    if (Coalesce(neighbor, node, parent, pos, transaction)) {
      transaction->AddIntoDeletedPageSet(parent->GetPageId());
    }
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    neighbor_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(neighbor->GetPageId(), true);
    return true;
  }

  if (pos != parent->GetSize() - 1) {
    auto neighbor_page = buffer_pool_manager_->FetchPage(parent->ValueAt(pos + 1));
    neighbor_page->WLatch();
    auto neighbor = reinterpret_cast<NodeType *>(neighbor_page->GetData());
    if (neighbor->GetSize() > neighbor->GetMinSize()) {
      Redistribute(node, neighbor, parent, pos + 1, false);
      ReleaseQueueLatch(transaction);
      buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
      neighbor_page->WUnlatch();
      buffer_pool_manager_->UnpinPage(neighbor->GetPageId(), true);
      return false;
    }
    transaction->AddIntoDeletedPageSet(neighbor->GetPageId());
    if (Coalesce(node, neighbor, parent, pos + 1, transaction)) {
      transaction->AddIntoDeletedPageSet(parent->GetPageId());
    }
    buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    neighbor_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(neighbor->GetPageId(), true);
    return false;
  }
  return false;
}

INDEX_TEMPLATE_ARGUMENTS
template <typename NodeType>
auto BPLUSTREE_TYPE::Coalesce(NodeType *left_node, NodeType *right_node, InternalPage *parent, int pos,
                              Transaction *transaction) -> bool {
  if (right_node->IsLeafPage()) {
    reinterpret_cast<LeafPage *>(left_node)->Merge(reinterpret_cast<LeafPage *>(right_node));
  } else {
    reinterpret_cast<InternalPage *>(left_node)->Merge(reinterpret_cast<InternalPage *>(right_node), parent->KeyAt(pos),
                                                       buffer_pool_manager_);
  }
  parent->Delete(pos);
  return CoalesceOrRedistribute(parent, transaction);
}

INDEX_TEMPLATE_ARGUMENTS
template <typename NodeType>
void BPLUSTREE_TYPE::Redistribute(NodeType *left_node, NodeType *right_node, InternalPage *parent, int right_pos,
                                  bool from_previous) {
  if (left_node->IsLeafPage()) {
    auto left_leaf = reinterpret_cast<LeafPage *>(left_node);
    auto right_leaf = reinterpret_cast<LeafPage *>(right_node);
    if (from_previous) {
      left_leaf->ShiftTo(right_leaf);
    } else {
      left_leaf->ShiftFrom(right_leaf);
    }
    parent->SetKeyAt(right_pos, right_leaf->KeyAt(0));

  } else {
    auto left_internal = reinterpret_cast<InternalPage *>(left_node);
    auto right_internal = reinterpret_cast<InternalPage *>(right_node);
    if (from_previous) {
      left_internal->ShiftTo(right_internal, buffer_pool_manager_);
    } else {
      left_internal->ShiftFrom(right_internal, buffer_pool_manager_);
    }
    parent->SetKeyAt(right_pos, right_internal->KeyAt(0));
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  root_latch_.WLock();
  transaction->AddIntoPageSet(nullptr);
  if (IsEmpty()) {
    return ReleaseQueueLatch(transaction);
  }
  auto leaf_page = FindLeaf(key, FindType::kDelete, transaction);
  auto leaf = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int pos = GetPosOnLeafPage(leaf, key);
  if (pos == -1 || comparator_(key, leaf->KeyAt(pos)) != 0) {
    ReleaseQueueLatch(transaction);
    leaf_page->WUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    return;
  }
  leaf->Delete(pos);

  if (CoalesceOrRedistribute(leaf, transaction)) {
    transaction->AddIntoDeletedPageSet(leaf->GetPageId());
  }
  leaf_page->WUnlatch();
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  for (auto page_id : *transaction->GetDeletedPageSet()) {
    buffer_pool_manager_->DeletePage(page_id);
  }
  transaction->GetDeletedPageSet()->clear();
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE(buffer_pool_manager_, nullptr, 0);
  }
  int page_id = GetRootPageId();
  while (true) {
    auto page = buffer_pool_manager_->FetchPage(page_id);
    BUSTUB_ASSERT(page != nullptr, "failed to fetch page");
    auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (node->IsLeafPage()) {
      return INDEXITERATOR_TYPE(buffer_pool_manager_, reinterpret_cast<LeafPage *>(node), 0);
    }
    assert(0 < node->GetSize());
    int next_page_id = reinterpret_cast<InternalPage *>(node)->ValueAt(0);
    buffer_pool_manager_->UnpinPage(page_id, false);
    page_id = next_page_id;
  }
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE {
  auto page_id = GetRootPageId();
  auto page = buffer_pool_manager_->FetchPage(page_id);
  while (true) {
    BUSTUB_ASSERT(page != nullptr, "failed to fetch page");
    auto node = reinterpret_cast<BPlusTreePage *>(page->GetData());
    if (node->IsLeafPage()) {
      break;
    }
    auto internal_node = reinterpret_cast<InternalPage *>(node);
    int pos = GetPosOnInternalPage(internal_node, key);
    assert(0 <= pos && pos < internal_node->GetSize());
    auto next_page_id = internal_node->ValueAt(pos);
    buffer_pool_manager_->UnpinPage(page_id, false);
    page_id = next_page_id;
    page = buffer_pool_manager_->FetchPage(page_id);
  }
  auto leaf = reinterpret_cast<LeafPage *>(page->GetData());

  int pos = GetPosOnLeafPage(leaf, key);
  return INDEXITERATOR_TYPE(buffer_pool_manager_, leaf, pos);
}

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(buffer_pool_manager_, nullptr, 0); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
