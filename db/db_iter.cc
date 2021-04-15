// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_iter.h"

#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/filename.h"
#include "leveldb/env.h"
#include "leveldb/iterator.h"
#include "port/port.h"
#include "util/logging.h"
#include "util/mutexlock.h"
#include "util/random.h"

namespace leveldb {

#if 0
static void DumpInternalIter(Iterator* iter) {
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey k;
    if (!ParseInternalKey(iter->key(), &k)) {
      std::fprintf(stderr, "Corrupt '%s'\n", EscapeString(iter->key()).c_str());
    } else {
      std::fprintf(stderr, "@ '%s'\n", k.DebugString().c_str());
    }
  }
}
#endif

namespace {

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
class DBIter : public Iterator {
 public:
  // Which direction is the iterator currently moving?
  // (1) When moving forward, the internal iterator is positioned at
  //     the exact entry that yields this->key(), this->value()
  // (2) When moving backwards, the internal iterator is positioned
  //     just before all entries whose user key == this->key().
  enum Direction { kForward, kReverse };

  DBIter(DBImpl* db, const Comparator* cmp, Iterator* iter, SequenceNumber s,
         uint32_t seed)
      : db_(db),
        user_comparator_(cmp),
        iter_(iter),
        sequence_(s),
        direction_(kForward),
        valid_(false),
        rnd_(seed),
        bytes_until_read_sampling_(RandomCompactionPeriod()) {}

  DBIter(const DBIter&) = delete;

  DBIter& operator=(const DBIter&) = delete;

  ~DBIter() override { delete iter_; }
  
  bool Valid() const override { return valid_; }

  //  kForward直接取iter_->key()，否则取saved key
  Slice key() const override {
    assert(valid_);
    return (direction_ == kForward) ? ExtractUserKey(iter_->key()) : saved_key_;
  }
  //  kForward直接取iter_->value()，否则取saved value
  Slice value() const override {
    assert(valid_);
    return (direction_ == kForward) ? iter_->value() : saved_value_;
  }

  Status status() const override {
    if (status_.ok()) {
      return iter_->status();
    } else {
      return status_;
    }
  }

  void Next() override;
  void Prev() override;
  void Seek(const Slice& target) override;
  void SeekToFirst() override;
  void SeekToLast() override;

 private:
  void FindNextUserEntry(bool skipping, std::string* skip);
  void FindPrevUserEntry();
  bool ParseKey(ParsedInternalKey* key);

  inline void SaveKey(const Slice& k, std::string* dst) {
    dst->assign(k.data(), k.size());
  }

  inline void ClearSavedValue() {
    if (saved_value_.capacity() > 1048576) {
      std::string empty;
      swap(empty, saved_value_);
    } else {
      saved_value_.clear();
    }
  }

  // Picks the number of bytes that can be read until a compaction is scheduled.
  size_t RandomCompactionPeriod() {
    return rnd_.Uniform(2 * config::kReadBytesPeriod);
  }

  DBImpl* db_;
  const Comparator* const user_comparator_;
  Iterator* const iter_;
  SequenceNumber const sequence_;
  Status status_;
  std::string saved_key_;    // == current key when direction_==kReverse
  std::string saved_value_;  // == current raw value when direction_==kReverse
  Direction direction_;
  bool valid_;
  Random rnd_;
  size_t bytes_until_read_sampling_;
};

inline bool DBIter::ParseKey(ParsedInternalKey* ikey) {
  Slice k = iter_->key();

  size_t bytes_read = k.size() + iter_->value().size();
  while (bytes_until_read_sampling_ < bytes_read) {
    bytes_until_read_sampling_ += RandomCompactionPeriod();
    db_->RecordReadSample(k);
  }
  assert(bytes_until_read_sampling_ >= bytes_read);
  bytes_until_read_sampling_ -= bytes_read;

  if (!ParseInternalKey(k, ikey)) {
    status_ = Status::Corruption("corrupted internal key in DBIter");
    return false;
  } else {
    return true;
  }
}

void DBIter::Next() {
  assert(valid_);

  if (direction_ == kReverse) {  // Switch directions?
    direction_ = kForward;
    // iter_刚好在this->key()的所有entry之前，所以先跳转到this->key()
    // 的entries范围之内，然后再做常规的skip.
    // iter_ is pointing just before the entries for this->key(),
    // so advance into the range of entries for this->key() and then
    // use the normal skipping code below.
    if (!iter_->Valid()) {
      iter_->SeekToFirst();
    } else {
      iter_->Next();
    }
    if (!iter_->Valid()) {
      valid_ = false;
      saved_key_.clear();
      return;
    }
    // saved_key_ already contains the key to skip past.
  } else {
    // Store in saved_key_ the current key so we skip it below.
    // 把saved_key_ 用作skip的临时存储空间
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);

    // iter_ is pointing to current key. We can now safely move to the next to
    // avoid checking current key.
    iter_->Next();
    if (!iter_->Valid()) {
      valid_ = false;
      saved_key_.clear();
      return;
    }
  }

  FindNextUserEntry(true, &saved_key_);
}

// 参数@skipping 表明是否要跳过sequence更小的entry；
// 参数@skip 临时存储空间，保存seek时要跳过的key;
// FindNextUserKey确保定位到的entry的sequence不会大于指定的sequence，并跳过被删除标记覆盖的旧记录
void DBIter::FindNextUserEntry(bool skipping, std::string* skip) {
  // Loop until we hit an acceptable entry to yield
  assert(iter_->Valid());
  assert(direction_ == kForward);
  do {
    ParsedInternalKey ikey;
    // 确保iter_->key()的sequence <= 遍历指定的sequence
    if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
      switch (ikey.type) {
        case kTypeDeletion:
          // Arrange to skip all upcoming entries for this key since
          // they are hidden by this deletion.
          //  对于该key，跳过后面遇到的所有entry，它们被这次删除覆盖了
          //  保存key到skip中，并设置skipping=true
          SaveKey(ikey.user_key, skip);
          skipping = true;
          break;
        case kTypeValue:
          if (skipping &&
              user_comparator_->Compare(ikey.user_key, *skip) <= 0) {
            // 这是一个被删除覆盖的entry，或者user key比指定的key小，跳过
            // Entry hidden
          } else { // 找到，清空saved key并返回，iter_已定位到正确的entry
            valid_ = true;
            saved_key_.clear();
            return;
          }
          break;
      }
    }
    iter_->Next(); // 继续检查下一个entry
  } while (iter_->Valid());
  // 到这里表明已经找到最后了，没有符合的entry
  saved_key_.clear();
  valid_ = false;
}

void DBIter::Prev() {
  assert(valid_);

  if (direction_ == kForward) {  // Switch directions?  //需要预处理，并更改direction
    // iter_ is pointing at the current entry.  Scan backwards until
    // the key changes so we can use the normal reverse scanning code.
    // iter_指向当前的entry，向后扫描直到key发生改变，然后我们可以做常规的reverse扫描
    assert(iter_->Valid());  // Otherwise valid_ would have been false
    SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
    while (true) {
      iter_->Prev();
      if (!iter_->Valid()) { // 到头了，直接返回
        valid_ = false;
        saved_key_.clear();
        ClearSavedValue();
        return;
      }
      if (user_comparator_->Compare(ExtractUserKey(iter_->key()), saved_key_) <
          0) {
        break; // key变化就跳出循环，此时iter_刚好位于saved key对应的所有entry之前
      }
    }
    direction_ = kReverse;
  }

  FindPrevUserEntry();
}
// 根据指定的sequence，依次检查前一个entry，直到遇到user key小于saved key，并且类型不是Delete的entry。
void DBIter::FindPrevUserEntry() {
  assert(direction_ == kReverse);  // 确保是kReverse方向

  ValueType value_type = kTypeDeletion;  //后面的循环至少执行一次Prev操作
  if (iter_->Valid()) {
    do {
      // 确保iter_->key()的sequence <= 遍历指定的sequence
      ParsedInternalKey ikey;
      if (ParseKey(&ikey) && ikey.sequence <= sequence_) {
        if ((value_type != kTypeDeletion) &&
            user_comparator_->Compare(ikey.user_key, saved_key_) < 0) {
          // We encountered a non-deleted value in entries for previous keys,
          break;  // 我们遇到了前一个key的一个未被删除的entry，跳出循环
                  // 此时Key()将返回saved_key，saved key非空
        }

        //根据类型，如果是Deletion则清空saved key和saved value
        //否则，把iter_的user key和value赋给saved key和saved value
        value_type = ikey.type;
        if (value_type == kTypeDeletion) {
          saved_key_.clear();
          ClearSavedValue();
        } else {
          Slice raw_value = iter_->value();
          if (saved_value_.capacity() > raw_value.size() + 1048576) {
            std::string empty;
            swap(empty, saved_value_);
          }
          SaveKey(ExtractUserKey(iter_->key()), &saved_key_);
          saved_value_.assign(raw_value.data(), raw_value.size());
        }
      }
      iter_->Prev();
    } while (iter_->Valid());
  }

  if (value_type == kTypeDeletion) { // 表明遍历结束了，将direction设置为kForward
    // End
    valid_ = false;
    saved_key_.clear();
    ClearSavedValue();
    direction_ = kForward;
  } else {
    valid_ = true;
  }
}

void DBIter::Seek(const Slice& target) {
  direction_ = kForward; // 向前seek
  // 清空saved value和saved key，并根据target设置saved key
  ClearSavedValue();
  saved_key_.clear();
  // kValueTypeForSeek(1) > kDeleteType(0)
  AppendInternalKey(&saved_key_, 
                    ParsedInternalKey(target, sequence_, kValueTypeForSeek));
  // iter seek到saved key
  iter_->Seek(saved_key_); 

  //可以定位到合法的iter，还需要跳过Delete的entry
  if (iter_->Valid()) {
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToFirst() {
  direction_ = kForward;// 向前seek
  // 清空saved value，首先iter_->SeekToFirst，然后跳过Delete的entry
  ClearSavedValue();

  iter_->SeekToFirst();
  if (iter_->Valid()) {
    FindNextUserEntry(false, &saved_key_ /* temporary storage */);
  } else {
    valid_ = false;
  }
}

void DBIter::SeekToLast() {
  direction_ = kReverse;
  ClearSavedValue();
  iter_->SeekToLast();
  FindPrevUserEntry();
}

}  // anonymous namespace

Iterator* NewDBIterator(DBImpl* db, const Comparator* user_key_comparator,
                        Iterator* internal_iter, SequenceNumber sequence,
                        uint32_t seed) {
  return new DBIter(db, user_key_comparator, internal_iter, sequence, seed);
}

}  // namespace leveldb
