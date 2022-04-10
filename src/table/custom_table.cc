#include "custom_table.h"
#include <cstring>

namespace bytedance_db_project {
CustomTable::CustomTable() {}

CustomTable::~CustomTable() {}

void CustomTable::Load(BaseDataLoader *loader) {
  // TODO: Implement this!
  num_cols_ = loader->GetNumCols();
  auto rows = loader->GetCustomRows();
  num_rows_ = rows.size();
  rows_ = new char[FIXED_CUSTOM_FIELD_LEN * num_rows_ * num_cols_];
  index_cols_ = new char[FIXED_CUSTOM_FIELD_LEN * num_rows_ * 3];
  for (auto row_id = 0; row_id < num_rows_; row_id++) {
    auto cur_row = rows.at(row_id);
    // rows
    std::memcpy(rows_ + row_id * (FIXED_CUSTOM_FIELD_LEN * num_cols_), cur_row,
                FIXED_CUSTOM_FIELD_LEN * num_cols_);
    // index_cols
    // std::memcpy(index_cols_ + row_id * (FIXED_CUSTOM_FIELD_LEN * 3), cur_row,
    //             FIXED_CUSTOM_FIELD_LEN * 3);   
    for (int32_t col_id = 0; col_id < 3; col_id++) {
      int32_t offset = FIXED_FIELD_LEN * ((col_id * num_rows_) + row_id);
      std::memcpy(index_cols_ + offset, cur_row + FIXED_FIELD_LEN * col_id,
                  FIXED_FIELD_LEN);
    }
    // index         
    index_.insert(std::pair<int,int>(*(rows_ + row_id * (FIXED_CUSTOM_FIELD_LEN * num_cols_)), row_id));
  }
}

int32_t CustomTable::GetIntField(int32_t row_id, int32_t col_id) {
  // TODO: Implement this!
  // return rows_[FIXED_CUSTOM_FIELD_LEN*(row_id*num_cols_+col_id)];
  return *(int32_t*)(rows_ + FIXED_CUSTOM_FIELD_LEN*(row_id*num_cols_+col_id));
}

void CustomTable::PutIntField(int32_t row_id, int32_t col_id, int32_t field) {
  // TODO: Implement this!
  // refresh index
  if(col_id == 0){
    int32_t num = *(int32_t*)(rows_ + FIXED_CUSTOM_FIELD_LEN*(row_id*num_cols_+col_id));
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

  // refresh index_cols
  if(col_id>=0 && col_id <=2){
    *(int32_t*)(index_cols_ + FIXED_CUSTOM_FIELD_LEN*(row_id + col_id * num_rows_)) = field;
  }
  *(int32_t*)(rows_ + FIXED_CUSTOM_FIELD_LEN*(row_id*num_cols_+col_id)) = field;
  return ;
}

int64_t CustomTable::ColumnSum() {
  // TODO: Implement this!
  int64_t res = 0;
  // for (auto it = index_.begin(); it != index_.end(); ++it){
  //   res += it->first;
  // }
  for (auto row_id = 0; row_id < num_rows_; row_id++){
    res += *(index_cols_ + row_id * FIXED_CUSTOM_FIELD_LEN );
  }
  return res;
}

int64_t CustomTable::PredicatedColumnSum(int32_t threshold1,
                                         int32_t threshold2) {
  // TODO: Implement this!
  int64_t res = 0;
  std::vector<bool> candidates(num_rows_, true);
  for (auto row_id = 0; row_id < num_rows_; row_id++){
    if(*(index_cols_ + (1 * num_rows_ + row_id) * FIXED_CUSTOM_FIELD_LEN ) <= threshold1){
      candidates[row_id] = false;
    }
  }
  for (auto row_id = 0; row_id < num_rows_; row_id++){
    if(*(index_cols_ + (2 * num_rows_ + row_id) * FIXED_CUSTOM_FIELD_LEN ) >= threshold2){
      candidates[row_id] = false;
    }
  }
  for (auto row_id = 0; row_id < num_rows_; row_id++){
    if(candidates[row_id]){
      res += *(int32_t*)(index_cols_ + row_id * FIXED_CUSTOM_FIELD_LEN );
    }
  }

  // for (auto row_id = 0; row_id < num_rows_; row_id++) {
  //   if((*(index_cols_ + row_id * (FIXED_CUSTOM_FIELD_LEN * 3) + FIXED_CUSTOM_FIELD_LEN) > threshold1) && (*(index_cols_ + row_id * (FIXED_CUSTOM_FIELD_LEN * 3) + FIXED_CUSTOM_FIELD_LEN * 2) < threshold2))
  //     res += *(index_cols_ + row_id * (FIXED_CUSTOM_FIELD_LEN * 3));
  // }
  return res;
}

int64_t CustomTable::PredicatedAllColumnsSum(int32_t threshold) {
  // TODO: Implement this!
  int64_t res = 0;
  auto it = index_.upper_bound(threshold);
  for (; it != index_.end(); ++it){
    for (auto col_id = 0; col_id < num_cols_; col_id++) {
      res += *(rows_ + it->second * (FIXED_CUSTOM_FIELD_LEN * num_cols_) + col_id * FIXED_CUSTOM_FIELD_LEN);
    }
  }
  return res;
}

int64_t CustomTable::PredicatedUpdate(int32_t threshold) {
  // TODO: Implement this!
  int64_t res = 0;
  auto up = index_.lower_bound(threshold);
  for (auto it = index_.begin(); it != up; ++it){
    res++;
    *(rows_ + it->second * (FIXED_CUSTOM_FIELD_LEN * num_cols_) + FIXED_CUSTOM_FIELD_LEN * 3) += *(rows_ + it->second * (FIXED_CUSTOM_FIELD_LEN * num_cols_) + FIXED_CUSTOM_FIELD_LEN * 2);
  }
  return res;
}
} // namespace bytedance_db_project