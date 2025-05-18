#pragma once

#include "sql/stmt/select_stmt.h"
#include "sql/operator/physical_operator.h"
#include "common/sys/rc.h"
#include "event/sql_event.h"

class SQLStageEvent;
class SelectExecutor
{
public:
  static RC execute(SQLStageEvent *sql_event);
};
