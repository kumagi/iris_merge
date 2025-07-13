#include "iris_merge.h"

#include <gtest/gtest.h>
#include "substrait/plan.pb.h"
#include "absl/status/status.h"

namespace iris_merge {

class IrisMergeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    temp_db_path_ = testing::TempDir() + "test.db";
  }

  std::string temp_db_path_;
};

TEST_F(IrisMergeTest, CreateTable) {
  IrisMerge iris_merge(temp_db_path_);
  const std::string table_name = "test_table";

  ::substrait::Plan plan;
  auto* rel = plan.add_relations();
  auto* ddl = rel->mutable_root()->mutable_input()->mutable_ddl();
  ddl->set_object(::substrait::DdlRel_DdlObject_DDL_OBJECT_TABLE);
  ddl->set_op(::substrait::DdlRel_DdlOp_DDL_OP_CREATE);
  auto* named_table = ddl->mutable_named_object();
  named_table->add_names(table_name);
  auto* s = ddl->mutable_table_schema();
  s->add_names("col1");
  s->add_names("col2");

  EXPECT_TRUE(iris_merge.ExecuteDML(plan).ok());
}

TEST_F(IrisMergeTest, DropTable) {
  IrisMerge iris_merge(temp_db_path_);
  const std::string table_name = "test_table";

  // Create Table
  {
    ::substrait::Plan plan;
    auto* rel = plan.add_relations();
    auto* ddl = rel->mutable_root()->mutable_input()->mutable_ddl();
    ddl->set_object(::substrait::DdlRel_DdlObject_DDL_OBJECT_TABLE);
    ddl->set_op(::substrait::DdlRel_DdlOp_DDL_OP_CREATE);
    auto* named_table = ddl->mutable_named_object();
    named_table->add_names(table_name);
    auto* s = ddl->mutable_table_schema();
    s->add_names("col1");
    s->add_names("col2");
    ASSERT_TRUE(iris_merge.ExecuteDML(plan).ok());
  }

  // Drop Table
  {
    ::substrait::Plan plan;
    auto* rel = plan.add_relations();
    auto* ddl = rel->mutable_root()->mutable_input()->mutable_ddl();
    ddl->set_object(::substrait::DdlRel_DdlObject_DDL_OBJECT_TABLE);
    ddl->set_op(::substrait::DdlRel_DdlOp_DDL_OP_DROP);
    auto* named_table = ddl->mutable_named_object();
    named_table->add_names(table_name);
    EXPECT_TRUE(iris_merge.ExecuteDML(plan).ok());
  }

  // Select from dropped table
  {
    ::substrait::Plan plan;
    auto* rel = plan.add_relations();
    auto* read = rel->mutable_root()->mutable_input()->mutable_read();
    read->mutable_named_table()->add_names(table_name);
    
    auto result = iris_merge.ExecuteDML(plan);
    EXPECT_EQ(result.status().code(), absl::StatusCode::kNotFound);
  }
}

TEST_F(IrisMergeTest, DeleteFromTable) {
  IrisMerge iris_merge(temp_db_path_);
  const std::string table_name = "test_table";

  // Create Table
  {
    ::substrait::Plan plan;
    auto* rel = plan.add_relations();
    auto* ddl = rel->mutable_root()->mutable_input()->mutable_ddl();
    ddl->set_object(::substrait::DdlRel_DdlObject_DDL_OBJECT_TABLE);
    ddl->set_op(::substrait::DdlRel_DdlOp_DDL_OP_CREATE);
    auto* named_table = ddl->mutable_named_object();
    named_table->add_names(table_name);
    auto* s = ddl->mutable_table_schema();
    s->add_names("col1");
    s->add_names("col2");
    ASSERT_TRUE(iris_merge.ExecuteDML(plan).ok());
  }

  // Insert into table
  {
    ::substrait::Plan plan;
    auto* rel = plan.add_relations();
    auto* write = rel->mutable_root()->mutable_input()->mutable_write();
    write->set_op(::substrait::WriteRel::WRITE_OP_INSERT);
    write->mutable_named_table()->add_names(table_name);

    auto* extension = write->mutable_input()->mutable_extension_leaf();
    ::substrait::Expression expression;
    auto* literal = expression.mutable_literal();
    auto* struct_ = literal->mutable_struct_();
    auto* field1 = struct_->add_fields();
    field1->set_i32(1);
    auto* field2 = struct_->add_fields();
    field2->set_string("hello");
    extension->mutable_detail()->PackFrom(expression);

    EXPECT_TRUE(iris_merge.ExecuteDML(plan).ok());
  }

  // Delete from table
  {
    ::substrait::Plan plan;
    auto* rel = plan.add_relations();
    auto* write = rel->mutable_root()->mutable_input()->mutable_write();
    write->set_op(::substrait::WriteRel::WRITE_OP_DELETE);
    write->mutable_named_table()->add_names(table_name);
    EXPECT_TRUE(iris_merge.ExecuteDML(plan).ok());
  }

  // Select from table
  {
    ::substrait::Plan plan;
    auto* rel = plan.add_relations();
    auto* read = rel->mutable_root()->mutable_input()->mutable_read();
    read->mutable_named_table()->add_names(table_name);
    
    auto result = iris_merge.ExecuteDML(plan);
    ASSERT_TRUE(result.ok());
    const auto& rows = *result;
    EXPECT_EQ(rows.size(), 0);
  }
}

TEST_F(IrisMergeTest, UpdateTable) {
  IrisMerge iris_merge(temp_db_path_);
  const std::string table_name = "test_table";

  // Create Table
  {
    ::substrait::Plan plan;
    auto* rel = plan.add_relations();
    auto* ddl = rel->mutable_root()->mutable_input()->mutable_ddl();
    ddl->set_object(::substrait::DdlRel_DdlObject_DDL_OBJECT_TABLE);
    ddl->set_op(::substrait::DdlRel_DdlOp_DDL_OP_CREATE);
    auto* named_table = ddl->mutable_named_object();
    named_table->add_names(table_name);
    auto* s = ddl->mutable_table_schema();
    s->add_names("col1");
    s->add_names("col2");
    ASSERT_TRUE(iris_merge.ExecuteDML(plan).ok());
  }

  ::substrait::Plan plan;
  auto* rel = plan.add_relations();
  auto* write = rel->mutable_root()->mutable_input()->mutable_write();
  write->set_op(::substrait::WriteRel::WRITE_OP_UPDATE);
  write->mutable_named_table()->add_names(table_name);
  
  auto result = iris_merge.ExecuteDML(plan);
  EXPECT_EQ(result.status().code(), absl::StatusCode::kUnimplemented);
}

TEST_F(IrisMergeTest, Persistence) {
  const std::string table_name = "test_table";
  {
    IrisMerge iris_merge(temp_db_path_);

    // Create Table
    ::substrait::Plan plan;
    auto* rel = plan.add_relations();
    auto* ddl = rel->mutable_root()->mutable_input()->mutable_ddl();
    ddl->set_object(::substrait::DdlRel_DdlObject_DDL_OBJECT_TABLE);
    ddl->set_op(::substrait::DdlRel_DdlOp_DDL_OP_CREATE);
    auto* named_table = ddl->mutable_named_object();
    named_table->add_names(table_name);
    auto* s = ddl->mutable_table_schema();
    s->add_names("col1");
    s->add_names("col2");
    ASSERT_TRUE(iris_merge.ExecuteDML(plan).ok());
  }

  {
    IrisMerge iris_merge(temp_db_path_);
    ::substrait::Plan plan;
    auto* rel = plan.add_relations();
    auto* read = rel->mutable_root()->mutable_input()->mutable_read();
    read->mutable_named_table()->add_names(table_name);
    
    auto result = iris_merge.ExecuteDML(plan);
    ASSERT_TRUE(result.ok());
  }
}

}  // namespace iris_merge
