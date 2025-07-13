#include "iris_merge.h"

#include <algorithm>
#include <fstream>
#include <sstream>
#include <chrono>
#include <google/protobuf/any.pb.h>
#include "absl/status/status.h"

namespace iris_merge {

// IrisMergeStorage::Table class implementation
IrisMergeStorage::Table::Table(const std::string& name, const std::vector<std::string>& schema)
    : name_(name), schema_(schema) {}

IrisMergeStorage::Table::~Table() = default;

const std::string& IrisMergeStorage::Table::GetName() const { return name_; }

const std::vector<std::string>& IrisMergeStorage::Table::GetSchema() const { return schema_; }

void IrisMergeStorage::Table::AddRow(const Row& row) { rows_.push_back(row); }

void IrisMergeStorage::Table::ClearRows() { rows_.clear(); }

const std::vector<Row>& IrisMergeStorage::Table::GetRows() const { return rows_; }

// IrisMergeStorage class implementation
IrisMergeStorage::IrisMergeStorage(const std::string& filepath) : filepath_(filepath) {
  Load().IgnoreError();
}

IrisMergeStorage::~IrisMergeStorage() = default;

absl::Status IrisMergeStorage::Save() {
  std::ofstream ofs(filepath_);
  if (!ofs) {
    return absl::InternalError("Failed to open file for writing.");
  }
  for (const auto& table : tables_) {
    ofs << table->GetName() << ":";
    const auto& schema = table->GetSchema();
    for (size_t i = 0; i < schema.size(); ++i) {
      ofs << schema[i] << (i == schema.size() - 1 ? "" : ",");
    }
    ofs << std::endl;
  }
  return absl::OkStatus();
}

absl::Status IrisMergeStorage::Load() {
  std::ifstream ifs(filepath_);
  if (!ifs) {
    return absl::OkStatus(); // File might not exist yet.
  }
  std::string line;
  while (std::getline(ifs, line)) {
    std::stringstream ss(line);
    std::string table_name;
    std::getline(ss, table_name, ':');
    
    std::string schema_str;
    std::getline(ss, schema_str);
    
    std::vector<std::string> schema;
    std::stringstream schema_ss(schema_str);
    std::string col;
    while(std::getline(schema_ss, col, ',')) {
        schema.push_back(col);
    }
    CreateTable(table_name, schema);
  }
  return absl::OkStatus();
}

IrisMergeStorage::Table* IrisMergeStorage::CreateTable(const std::string& name,
                                         const std::vector<std::string>& schema) {
  if (GetTable(name) != nullptr) {
    return nullptr;  // Table already exists
  }

  auto table = std::make_unique<Table>(name, schema);
  tables_.push_back(std::move(table));
  return tables_.back().get();
}

IrisMergeStorage::Table* IrisMergeStorage::GetTable(const std::string& name) {
  for (const auto& table : tables_) {
    if (table->GetName() == name) {
      return table.get();
    }
  }
  return nullptr;
}

bool IrisMergeStorage::DropTable(const std::string& name) {
  auto it = std::remove_if(
      tables_.begin(), tables_.end(),
      [&](const std::unique_ptr<Table>& table) { return table->GetName() == name; });

  if (it != tables_.end()) {
    tables_.erase(it, tables_.end());
    return true;
  }

  return false;
}

absl::StatusOr<std::vector<Row>> IrisMergeStorage::ExecuteDML(const ::substrait::Plan& plan) {
  if (plan.relations_size() != 1) {
    return absl::InvalidArgumentError("Plan must have exactly one relation.");
  }
  const auto& rel = plan.relations(0);
  if (!rel.has_root()) {
    return absl::InvalidArgumentError("Relation must have a root.");
  }
  const auto& root = rel.root();
  if (!root.has_input()) {
    return absl::InvalidArgumentError("Root must have an input.");
  }
  const auto& input = root.input();
  if (input.has_ddl()) {
    const auto& ddl = input.ddl();
    if (ddl.op() == ::substrait::DdlRel_DdlOp_DDL_OP_CREATE &&
        ddl.object() == ::substrait::DdlRel_DdlObject_DDL_OBJECT_TABLE) {
      if (!ddl.has_named_object()) {
        return absl::InvalidArgumentError("CREATE TABLE must have a named object.");
      }
      const auto& named_object = ddl.named_object();
      if (named_object.names_size() != 1) {
        return absl::InvalidArgumentError("CREATE TABLE must have exactly one name.");
      }
      const std::string& table_name = named_object.names(0);
      if (!ddl.has_table_schema()) {
        return absl::InvalidArgumentError("CREATE TABLE must have a schema.");
      }
      const auto& schema = ddl.table_schema();
      std::vector<std::string> schema_names;
      for (const auto& name : schema.names()) {
        schema_names.push_back(name);
      }
      if (CreateTable(table_name, schema_names) == nullptr) {
        return absl::AlreadyExistsError("Table already exists.");
      }
      return std::vector<Row>{};
    } else if (ddl.op() == ::substrait::DdlRel_DdlOp_DDL_OP_DROP &&
               ddl.object() == ::substrait::DdlRel_DdlObject_DDL_OBJECT_TABLE) {
      if (!ddl.has_named_object()) {
        return absl::InvalidArgumentError("DROP TABLE must have a named object.");
      }
      const auto& named_object = ddl.named_object();
      if (named_object.names_size() != 1) {
        return absl::InvalidArgumentError("DROP TABLE must have exactly one name.");
      }
      const std::string& table_name = named_object.names(0);
      if (!DropTable(table_name)) {
        return absl::NotFoundError("Table not found.");
      }
      return std::vector<Row>{};
    }
  } else if (input.has_write()) {
    const auto& write = input.write();
    if (!write.has_named_table()) {
      return absl::InvalidArgumentError("WRITE must have a named table.");
    }
    const auto& named_table = write.named_table();
    if (named_table.names_size() != 1) {
      return absl::InvalidArgumentError("WRITE must have exactly one name.");
    }
    const std::string& table_name = named_table.names(0);
    Table* table = GetTable(table_name);
    if (table == nullptr) {
      return absl::NotFoundError("Table not found.");
    }

    switch (write.op()) {
      case ::substrait::WriteRel::WRITE_OP_UNSPECIFIED:
      case ::substrait::WriteRel::WRITE_OP_INSERT: {
        if (!write.has_input()) {
          return absl::InvalidArgumentError("INSERT must have an input.");
        }
        const auto& write_input = write.input();
        if (write_input.has_extension_leaf()) {
            const auto& extension = write_input.extension_leaf();
            if (extension.has_detail()) {
                ::substrait::Expression expression;
                if (extension.detail().UnpackTo(&expression)) {
                    if (expression.has_literal() && expression.literal().has_struct_()) {
                        const auto& literal = expression.literal().struct_();
                        Row row;
                        for (const auto& field : literal.fields()) {
                            if (field.has_i32()) {
                                row.push_back(field.i32());
                            } else if (field.has_string()) {
                                row.push_back(field.string());
                            } else {
                                return absl::InvalidArgumentError("Unsupported type in literal.");
                            }
                        }
                        table->AddRow(row);
                        return std::vector<Row>{};
                    }
                }
            }
        }
        return absl::InvalidArgumentError("Invalid INSERT operation.");
      }
      case ::substrait::WriteRel::WRITE_OP_DELETE:
        table->ClearRows();
        return std::vector<Row>{};
      case ::substrait::WriteRel::WRITE_OP_UPDATE:
        return absl::UnimplementedError("UPDATE not implemented.");
      default:
        return absl::UnimplementedError("Unsupported write operation.");
    }
  } else if (input.has_read()) {
    const auto& read = input.read();
    if (!read.has_named_table()) {
      return absl::InvalidArgumentError("READ must have a named table.");
    }
    const auto& named_table = read.named_table();
    if (named_table.names_size() != 1) {
      return absl::InvalidArgumentError("READ must have exactly one name.");
    }
    const std::string& table_name = named_table.names(0);
    Table* table = GetTable(table_name);
    if (table == nullptr) {
      return absl::NotFoundError("Table not found.");
    }
    return table->GetRows();
  }

  return absl::UnimplementedError("Unsupported DML operation.");
}

// IrisMerge class implementation
IrisMerge::IrisMerge(const std::string& filepath) : storage_(filepath) {
  saver_thread_ = std::thread(&IrisMerge::SavePeriodically, this);
}

IrisMerge::~IrisMerge() {
  {
    std::unique_lock<std::mutex> lock(mtx_);
    stop_saving_ = true;
  }
  cv_.notify_one();
  saver_thread_.join();
}

absl::StatusOr<std::vector<Row>> IrisMerge::ExecuteDML(const ::substrait::Plan& plan) {
  return storage_.ExecuteDML(plan);
}

void IrisMerge::SavePeriodically() {
  while (true) {
    std::unique_lock<std::mutex> lock(mtx_);
    if (cv_.wait_for(lock, std::chrono::seconds(1), [this] { return stop_saving_; })) {
      break;
    }
    storage_.Save().IgnoreError();
  }
}

}  // namespace iris_merge
