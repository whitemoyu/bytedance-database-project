#include "column_table.h"
#include <cstring>
#include <iostream>

namespace bytedance_db_project {
ColumnTable::ColumnTable() {}

ColumnTable::~ColumnTable() {
  if (columns_ != nullptr) {
    delete columns_;
    columns_ = nullptr;
  }
}

//
// columnTable, which stores data in column-major format.
// That is, data is laid out like
//   col 1 | col 2 | ... | col m.
//
void ColumnTable::Load(BaseDataLoader *loader) {
  num_cols_ = loader->GetNumCols();
  std::vector<char *> rows = loader->GetRows();
  num_rows_ = rows.size();
  columns_ = new char[FIXED_FIELD_LEN * num_rows_ * num_cols_];

  for (int32_t row_id = 0; row_id < num_rows_; row_id++) {
    auto cur_row = rows.at(row_id);
    for (int32_t col_id = 0; col_id < num_cols_; col_id++) {
      int32_t offset = FIXED_FIELD_LEN * ((col_id * num_rows_) + row_id);
      std::memcpy(columns_ + offset, cur_row + FIXED_FIELD_LEN * col_id,
                  FIXED_FIELD_LEN);
    }
  }
}

int32_t ColumnTable::GetIntField(int32_t row_id, int32_t col_id) {
  // TODO: Implement this!
  return *(int32_t*)(columns_ + FIXED_FIELD_LEN*(col_id*num_rows_+row_id));
}

void ColumnTable::PutIntField(int32_t row_id, int32_t col_id, int32_t field) {
  // TODO: Implement this!
  *(int32_t*)(columns_ + FIXED_FIELD_LEN*(col_id*num_rows_+row_id)) = field;
  return ;
}

int64_t ColumnTable::ColumnSum() {
  // TODO: Implement this!
  int64_t res = 0;
  for (auto col_id = 0; col_id < num_rows_; col_id++) {
    res += *(columns_ + col_id * (FIXED_FIELD_LEN));
  }
  return res;
}

int64_t ColumnTable::PredicatedColumnSum(int32_t threshold1,
                                         int32_t threshold2) {
  // TODO: Implement this!
  int64_t res = 0;
  for (auto row_id = 0; row_id < num_rows_; row_id++) {
    if((*(columns_ + 1 * (FIXED_FIELD_LEN * num_rows_) + row_id * FIXED_FIELD_LEN) > threshold1) && (*(columns_ + 2 * (FIXED_FIELD_LEN * num_rows_) + row_id * FIXED_FIELD_LEN) < threshold2))
      res += *(columns_ + row_id * FIXED_FIELD_LEN);
  }
  return res;
}

int64_t ColumnTable::PredicatedAllColumnsSum(int32_t threshold) {
  // TODO: Implement this!
  int64_t res = 0;
  for (auto row_id = 0; row_id < num_rows_; row_id++) {
    if(*(columns_ + row_id * FIXED_FIELD_LEN) > threshold){
      for (auto col_id = 0; col_id < num_cols_; col_id++) {
        res += *(columns_ + col_id * (FIXED_FIELD_LEN * num_rows_) + row_id * FIXED_FIELD_LEN);
      }
    }
  }
  return res;
}

int64_t ColumnTable::PredicatedUpdate(int32_t threshold) {
  // TODO: Implement this!
  int64_t res = 0;
  for (auto row_id = 0; row_id < num_rows_; row_id++) {
    if(*(columns_ + row_id * FIXED_FIELD_LEN) < threshold){
      res++;
      *(columns_ + 3 * (FIXED_FIELD_LEN * num_rows_) + row_id * FIXED_FIELD_LEN) += *(columns_ + 2 * (FIXED_FIELD_LEN * num_rows_) + row_id * FIXED_FIELD_LEN);
    }
  }
  return res;
}
} // namespace bytedance_db_project