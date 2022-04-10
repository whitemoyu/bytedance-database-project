#include "row_table.h"
#include <cstring>

namespace bytedance_db_project {
RowTable::RowTable() {}

RowTable::~RowTable() {
  if (rows_ != nullptr) {
    delete rows_;
    rows_ = nullptr;
  }
}

void RowTable::Load(BaseDataLoader *loader) {
  num_cols_ = loader->GetNumCols();
  auto rows = loader->GetRows();
  num_rows_ = rows.size();
  rows_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];
  for (auto row_id = 0; row_id < num_rows_; row_id++) {
    auto cur_row = rows.at(row_id);
    std::memcpy(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_), cur_row,
                FIXED_FIELD_LEN * num_cols_);
  }
}

int32_t RowTable::GetIntField(int32_t row_id, int32_t col_id) {
  // TODO: Implement this!
  return *(int32_t*)(rows_ + FIXED_FIELD_LEN*(row_id*num_cols_+col_id));
  //return 0;
}

void RowTable::PutIntField(int32_t row_id, int32_t col_id, int32_t field) {
  // TODO: Implement this!
  *(int32_t*)(rows_ + FIXED_FIELD_LEN*(row_id*num_cols_+col_id)) = field;
  return ;
}

int64_t RowTable::ColumnSum() {
  // TODO: Implement this!
  int64_t res = 0;
  for (auto row_id = 0; row_id < num_rows_; row_id++) {
    res += *(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_));
  }
  return res;
}

int64_t RowTable::PredicatedColumnSum(int32_t threshold1, int32_t threshold2) {
  // TODO: Implement this!
  int64_t res = 0;
  for (auto row_id = 0; row_id < num_rows_; row_id++) {
    if((*(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_) + FIXED_FIELD_LEN) > threshold1) && (*(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_) + FIXED_FIELD_LEN * 2) < threshold2))
      res += *(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_));
  }
  return res;
}

int64_t RowTable::PredicatedAllColumnsSum(int32_t threshold) {
  // TODO: Implement this!
  int64_t res = 0;
  for (auto row_id = 0; row_id < num_rows_; row_id++) {
    if(*(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_)) > threshold){
      for (auto col_id = 0; col_id < num_cols_; col_id++) {
        res += *(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_) + col_id * FIXED_FIELD_LEN);
      }
    }
  }
  return res;
}

int64_t RowTable::PredicatedUpdate(int32_t threshold) {
  // TODO: Implement this!
  int64_t res = 0;
  for (auto row_id = 0; row_id < num_rows_; row_id++) {
    if(*(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_)) < threshold){
      res++;
      *(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_) + FIXED_FIELD_LEN * 3) += *(rows_ + row_id * (FIXED_FIELD_LEN * num_cols_) + FIXED_FIELD_LEN*2);
    }
  }
  return res;
}
} // namespace bytedance_db_project