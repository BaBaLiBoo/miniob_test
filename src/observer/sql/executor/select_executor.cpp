#include "sql/executor/select_executor.h"
#include "event/sql_event.h"
#include "sql/stmt/select_stmt.h"
#include "sql/operator/project_physical_operator.h"
#include "sql/operator/table_scan_physical_operator.h"
#include "sql/operator/predicate_physical_operator.h"
#include "sql/operator/nested_loop_join_physical_operator.h"
#include "event/session_event.h"
#include "session/session.h"
#include "common/log/log.h"

RC SelectExecutor::execute(SQLStageEvent *sql_event)
{
  Stmt *stmt = sql_event->stmt();
  SelectStmt *select_stmt = static_cast<SelectStmt *>(stmt);
  SqlResult *sql_result = sql_event->session_event()->sql_result();

  const auto &tables = select_stmt->tables();
  if (tables.empty()) {
    LOG_WARN("No tables to select from");
    return RC::INVALID_ARGUMENT;
  }

  std::vector<std::unique_ptr<PhysicalOperator>> scan_operators;
  for (Table *table : tables) {
    scan_operators.emplace_back(
      std::make_unique<TableScanPhysicalOperator>(table, ReadWriteMode::READ_ONLY));
  }

  std::unique_ptr<PhysicalOperator> scan_op;
  if (scan_operators.size() == 1) {
    scan_op = std::move(scan_operators[0]);
  } else {
    scan_op = std::move(scan_operators[0]);
    for (size_t i = 1; i < scan_operators.size(); i++) {
      auto join = std::make_unique<NestedLoopJoinPhysicalOperator>();
      join->add_child(std::move(scan_op));
      join->add_child(std::move(scan_operators[i]));
      scan_op = std::move(join);
    }
  }

  if (select_stmt->filter_stmt() != nullptr) {
    scan_op = std::make_unique<PredicatePhysicalOperator>(std::move(scan_op), select_stmt->filter_stmt());
  }

  auto project_op = std::make_unique<ProjectPhysicalOperator>(
    std::move(select_stmt->query_expressions()));

  project_op->add_child(std::move(scan_op));
  sql_result->set_operator(std::move(project_op));
  return RC::SUCCESS;
}
