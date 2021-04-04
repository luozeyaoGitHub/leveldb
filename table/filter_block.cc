// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

// 把key添加到key_中，并在start_中记录位置
void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  start_.push_back(keys_.size());
  keys_.append(k.data(), k.size());
}

// 调用这个函数说明整个table的data block已经构建完了，可以生产最终的filter block了，
// 在TableBuilder::Finish函数中被调用，向sstable写入meta block
Slice FilterBlockBuilder::Finish() {
  //S1 如果start_数字不空，把为的key列表生产filter
  if (!start_.empty()) GenerateFilter();
  //S2 从0开始顺序存储各filter的偏移值，见filter block data的数据格式。
  const uint32_t array_offset =result_.size();
  for (size_t i = 0; i < filter_offsets_.size();i++) {
    PutFixed32(&result_,filter_offsets_[i]);
  }
  //S3 最后是filter个数，和shift常量（11），并返回结果
  PutFixed32(&result_,array_offset);
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}

void FilterBlockBuilder::GenerateFilter() {
  //  S1 如果filter中key个数为0，则直接压入result_.size()并返回
  const size_t num_keys = start_.size();
  if (num_keys == 0) {
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  //  S2 从key创建临时key list，根据key的序列字符串kyes_和各key在keys_中的开始位置start_依次提取出key
  start_.push_back(keys_.size());  // Simplify length computation
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    const char* base = keys_.data() + start_[i];
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  //  S3 为当前的key集合生产filter，并append到result_
  filter_offsets_.push_back(result_.size());
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);
  //  S4 清空，重置状态
  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
  size_t n = contents.size();
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  base_lg_ = contents[n - 1]; // 最后1byte存的是base
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5); //偏移数组的位置
  if (last_word > n - 5) return;
  data_ = contents.data();
  offset_ = data_ + last_word; // 偏移数组开始指针
  num_ = (n - 5 - last_word) / 4; // 计算出filter个数
}

// @block_offset是查找data block在sstable中的偏移，Filter根据此偏移计算filter的编号；
// @key是查找的key
bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  uint64_t index = block_offset >> base_lg_; // 计算出filter index
  if (index < num_) {
    uint32_t start = DecodeFixed32(offset_ + index * 4); // 解析出filter的range
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      Slice filter = Slice(data_ + start, limit - start); // 根据range得到filter
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false; // 空filter不匹配任何key
    }
  }
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
