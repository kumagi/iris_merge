#ifndef INCLUDE_IRIS_MERGE_H_
#define INCLUDE_IRIS_MERGE_H_

#include <string>
#include <vector>
#include <memory>
#include <variant>
#include <thread>
#include <mutex>
#include <condition_variable>

#include "absl/status/statusor.h"
#include "absl/status/status.h"
#include "substrait/plan.pb.h"

namespace iris_merge {

using Row = std::vector<std::variant<int32_t, std::string>>;

class IrisMergeStorage {
public:
    explicit IrisMergeStorage(const std::string& filepath);
    ~IrisMergeStorage();

    absl::StatusOr<std::vector<Row>> ExecuteDML(const ::substrait::Plan& plan);
    absl::Status Save();

private:
    class Table {
    public:
        Table(const std::string& name, const std::vector<std::string>& schema);
        ~Table();

        const std::string& GetName() const;
        const std::vector<std::string>& GetSchema() const;
        void AddRow(const Row& row);
        void ClearRows();
        const std::vector<Row>& GetRows() const;

    private:
        std::string name_;
        std::vector<std::string> schema_;
        std::vector<Row> rows_;
    };

    absl::Status Load();

    Table* CreateTable(const std::string& name,
                              const std::vector<std::string>& schema);
    Table* GetTable(const std::string& name);
    bool DropTable(const std::string& name);

    std::string filepath_;
    std::vector<std::unique_ptr<Table>> tables_;
};

class IrisMerge {
public:
    explicit IrisMerge(const std::string& filepath);
    ~IrisMerge();

    absl::StatusOr<std::vector<Row>> ExecuteDML(const ::substrait::Plan& plan);

    IrisMergeStorage storage_;
private:
    void SavePeriodically();

    std::thread saver_thread_;
    std::mutex mtx_;
    std::condition_variable cv_;
    bool stop_saving_ = false;
};

} // namespace iris_merge

#endif // INCLUDE_IRIS_MERGE_H_
