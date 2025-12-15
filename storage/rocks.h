
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rocksdb/c.h"

////////////////////////////////
uint64_t rocks_daaScoreLatest = 0;

typedef struct {
  uint64_t dtl;
} rocks_filterArg_t;

static void rocks_filterDestroy(void* arg) { free(arg); }

static const char* rocks_filterName(void* arg) { return "filterUDT"; }

static unsigned char rocks_filterFilter(void* arg, int level, const char* key, size_t key_length, const char* existing_value, size_t value_length, char** new_value, size_t* new_value_length, unsigned char* value_changed) {
  if (arg==NULL) return 0;
  if (value_length<8) return 1;
  rocks_filterArg_t* filterArg = (rocks_filterArg_t*)arg;
  if (rocks_daaScoreLatest==0 || filterArg->dtl==0) return 0;
  uint64_t daaScore;
  memcpy(&daaScore, existing_value, 8);
  if (rocks_daaScoreLatest>daaScore && daaScore>0 && rocks_daaScoreLatest-daaScore>filterArg->dtl) return 1;
  return 0;
}

static rocksdb_compactionfilter_t* rocks_newCompactionFilter(uint64_t dtl) {
  rocks_filterArg_t* arg = (rocks_filterArg_t*)malloc(sizeof(rocks_filterArg_t));
  arg->dtl = dtl;
  return rocksdb_compactionfilter_create(arg, rocks_filterDestroy, rocks_filterFilter, rocks_filterName);
}
