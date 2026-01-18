#include "Workload.h"
#include "local/ISchema.h"
#include "db/access/IndexBTreeObject.h"

namespace arboretum {

void Workload::InitSchema(std::istream &in) {
  assert(sizeof(uint64_t) == 8);
  assert(sizeof(double) == 8);
  std::string line;
  ISchema *schema;
  uint32_t num_tables = 0;
  while (getline(in, line)) {
    if (line.compare(0, 6, "TABLE=") == 0) {
      std::string tname;
      tname = &line[6];
      getline(in, line);
      int col_count = 0;
      // Read all fields for this table.
      std::vector<std::string> lines;
      while (line.length() > 1) {
        lines.push_back(line);
        getline(in, line);
      }
      schema = new(MemoryAllocator::Alloc(sizeof(ISchema), 64)) ISchema(tname, lines.size());
      for (uint32_t i = 0; i < lines.size(); i++) {
        std::string line = lines[i];
        size_t pos = 0;
        std::string token;
        int elem_num = 0;
        int size = 0;
        bool is_pkey = false;
        // string type;
        DataType type;
        std::string name;
        while (line.length() != 0) {
          pos = line.find(',');
          if (pos == std::string::npos)
            pos = line.length();
          token = line.substr(0, pos);
          line.erase(0, pos + 1);
          switch (elem_num) {
            case 0: size = atoi(token.c_str());
              break;
            case 1: type = StringToDataType(token);
              break;
            case 2: name = token;
              break;
            case 3: is_pkey = atoi(token.c_str()) == 1;
              break;
            default: assert(false);
          }
          elem_num++;
        }
        assert(elem_num == 4);
        schema->AddCol(name, size, type, is_pkey);
        col_count++;
      }
      auto cur_tab = db_->CreateTable(tname, schema);
      num_tables++;
      tables_[tname] = cur_tab;
      schemas_.push_back(schema);
    } else if (!line.compare(0, 6, "INDEX=")) {
      std::string iname;
      iname = &line[6];
      getline(in, line);
      std::vector<std::string> items;
      std::string token;
      size_t pos;
      while (line.length() != 0) {
        pos = line.find(',');
        if (pos == std::string::npos)
          pos = line.length();
        token = line.substr(0, pos);
        items.push_back(token);
        line.erase(0, pos + 1);
      }
      std::string tname(items[0]);
      OID table_id = tables_[tname];
      OID col_id = std::stoi(items[1]);
      if (g_index_type == IndexType::BTREE) {
        auto index = db_->CreateIndex(table_id, col_id, IndexType::BTREE, g_buf_type);
        indexes_.push_back(index);
      } else if (g_index_type == IndexType::TWOTREE) {
        auto top_index = db_->CreateIndex(table_id, col_id, IndexType::BTREE, OBJBUF);
        indexes_.push_back(top_index);
        auto bottom_index = db_->CreateIndex(table_id, col_id, IndexType::BTREE, PGBUF);
        indexes_.push_back(bottom_index);
      } else {
        indexes_.push_back(indexes_.size());
      }
    }
  }

}

} // arboretum
