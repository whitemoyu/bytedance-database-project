#include "indexed_row_table.h"
#include <cstring>

namespace bytedance_db_project {
IndexedRowTable::IndexedRowTable(int32_t index_column) {
  index_column_ = index_column;
}

void IndexedRowTable::Load(BaseDataLoader *loader) {
  // TODO: Implement this!
  num_cols_ = loader->GetNumCols();
  auto rows = loader->GetRows();
  num_rows_ = rows.size();
  rows_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];
  for (auto row_id = 0; row_id < num_rows_; row_id++) {
    auto cur_row = rows.at(row_id);
    // save the data in tree.
    // std::vector<int> row_index(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_), rows_ + row_id * (FIXED_FIELD_LEN * num_cols_) + num_cols_);
    // index_[*(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_))] = row_index;    
    std::memcpy(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_), cur_row,
                FIXED_FIELD_LEN * num_cols_);
    //add index
    index_.insert(std::pair<int,int>(*(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_) + index_column_ * FIXED_FIELD_LEN), row_id));
  }

}

int32_t IndexedRowTable::GetIntField(int32_t row_id, int32_t col_id) {
  // TODO: Implement this!
  return *(int32_t*)(rows_ + FIXED_FIELD_LEN*(row_id*num_cols_+col_id));
}

void IndexedRowTable::PutIntField(int32_t row_id, int32_t col_id,
                                  int32_t field) {
  // TODO: Implement this!
  if(col_id == index_column_){
    int32_t num = *(int32_t*)(rows_ + FIXED_FIELD_LEN*(row_id*num_cols_+col_id));
    auto it = index_.find(num);
    while (it->first == num){
      if (it->second == row_id)
      {
        break;
      }
      it++;
    }
    index_.erase(it);
    index_.insert(std::pair<int32_t, int32_t>(field, row_id));
  }
  *(int32_t*)(rows_ + FIXED_FIELD_LEN*(row_id*num_cols_+col_id)) = field;
  return ;
}

int64_t IndexedRowTable::ColumnSum() {
  // TODO: Implement this!
  int64_t res = 0;
  if(0 == index_column_){
    for (auto it = index_.begin(); it != index_.end(); ++it){
      res += it->first;
    }
  }else{
    for (auto row_id = 0; row_id < num_rows_; row_id++) {
      res += *(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_));
    }
  }
  return res;
}

int64_t IndexedRowTable::PredicatedColumnSum(int32_t threshold1,
                                             int32_t threshold2) {
  // TODO: Implement this!
  int64_t res = 0;
  for (auto row_id = 0; row_id < num_rows_; row_id++) {
    if((*(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_) + FIXED_FIELD_LEN) > threshold1) && (*(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_) + FIXED_FIELD_LEN * 2) < threshold2))
      res += *(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_));
  }
  return res;
}

int64_t IndexedRowTable::PredicatedAllColumnsSum(int32_t threshold) {
  // TODO: Implement this!
  int64_t res = 0;
  if(0 == index_column_){
    auto it = index_.upper_bound(threshold);
    for (; it != index_.end(); ++it){
      for (auto col_id = 0; col_id < num_cols_; col_id++) {
        res += *(rows_ + it->second * (FIXED_FIELD_LEN * num_cols_) + col_id * FIXED_FIELD_LEN);
      }
    }
  }else{
    for (auto row_id = 0; row_id < num_rows_; row_id++) {
      if(*(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_)) > threshold){
        for (auto col_id = 0; col_id < num_cols_; col_id++) {
          res += *(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_) + col_id * FIXED_FIELD_LEN);
        }
      }
    }
  }
  return res;
}

int64_t IndexedRowTable::PredicatedUpdate(int32_t threshold) {
  // TODO: Implement this!
  int64_t res = 0;
  if(0 == index_column_){
    auto up = index_.lower_bound(threshold);
    for (auto it = index_.begin(); it != up; ++it){
      res++;
      *(rows_ + it->second * (FIXED_FIELD_LEN * num_cols_) + FIXED_FIELD_LEN * 3) += *(rows_ + it->second * (FIXED_FIELD_LEN * num_cols_) + FIXED_FIELD_LEN * 2);
    }
  }else{
    for (auto row_id = 0; row_id < num_rows_; row_id++) {
      if(*(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_)) < threshold){
        res++;
        *(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_) + FIXED_FIELD_LEN * 3) += *(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_) + FIXED_FIELD_LEN*2);
      }
    }
  }
  return res;
}
} // namespace bytedance_db_project