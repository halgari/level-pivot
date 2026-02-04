/**
 * level_pivot - PostgreSQL Foreign Data Wrapper for LevelDB
 *
 * Exposes LevelDB key-value data as relational tables with pivot semantics.
 * Transforms hierarchical keys into rows where key segments become identity
 * columns and the final segment (attr) pivots into columns.
 */

// Windows compatibility: include winsock before PostgreSQL headers
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "access/reloptions.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "utils/rel.h"

PG_MODULE_MAGIC;

/* Handler and validator functions */
extern Datum level_pivot_fdw_handler(PG_FUNCTION_ARGS);
extern Datum level_pivot_fdw_validator(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(level_pivot_fdw_handler);
PG_FUNCTION_INFO_V1(level_pivot_fdw_validator);

/* FDW callback declarations (implemented in fdw_handler.cpp) */
extern void levelPivotGetForeignRelSize(PlannerInfo *root,
                                        RelOptInfo *baserel,
                                        Oid foreigntableid);
extern void levelPivotGetForeignPaths(PlannerInfo *root,
                                      RelOptInfo *baserel,
                                      Oid foreigntableid);
extern ForeignScan *levelPivotGetForeignPlan(PlannerInfo *root,
                                             RelOptInfo *baserel,
                                             Oid foreigntableid,
                                             ForeignPath *best_path,
                                             List *tlist,
                                             List *scan_clauses,
                                             Plan *outer_plan);
extern void levelPivotBeginForeignScan(ForeignScanState *node, int eflags);
extern TupleTableSlot *levelPivotIterateForeignScan(ForeignScanState *node);
extern void levelPivotReScanForeignScan(ForeignScanState *node);
extern void levelPivotEndForeignScan(ForeignScanState *node);

extern void levelPivotExplainForeignScan(ForeignScanState *node,
                                         ExplainState *es);

extern void levelPivotAddForeignUpdateTargets(PlannerInfo *root,
                                              Index rtindex,
                                              RangeTblEntry *target_rte,
                                              Relation target_relation);
extern List *levelPivotPlanForeignModify(PlannerInfo *root,
                                         ModifyTable *plan,
                                         Index resultRelation,
                                         int subplan_index);
extern void levelPivotBeginForeignModify(ModifyTableState *mtstate,
                                         ResultRelInfo *rinfo,
                                         List *fdw_private,
                                         int subplan_index,
                                         int eflags);
extern TupleTableSlot *levelPivotExecForeignInsert(EState *estate,
                                                   ResultRelInfo *rinfo,
                                                   TupleTableSlot *slot,
                                                   TupleTableSlot *planSlot);
extern TupleTableSlot *levelPivotExecForeignUpdate(EState *estate,
                                                   ResultRelInfo *rinfo,
                                                   TupleTableSlot *slot,
                                                   TupleTableSlot *planSlot);
extern TupleTableSlot *levelPivotExecForeignDelete(EState *estate,
                                                   ResultRelInfo *rinfo,
                                                   TupleTableSlot *slot,
                                                   TupleTableSlot *planSlot);
extern void levelPivotEndForeignModify(EState *estate, ResultRelInfo *rinfo);
extern int levelPivotIsForeignRelUpdatable(Relation rel);

extern List *levelPivotImportForeignSchema(ImportForeignSchemaStmt *stmt,
                                           Oid serverOid);

/* Validator function (implemented in fdw_validator.cpp) */
extern void levelPivotValidateOptions(List *options_list, Oid catalog);

} /* extern "C" */

/**
 * FDW handler function
 *
 * Returns the FdwRoutine structure containing pointers to all FDW callbacks.
 */
Datum
level_pivot_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);

    /* Scan callbacks (required) */
    fdwroutine->GetForeignRelSize = levelPivotGetForeignRelSize;
    fdwroutine->GetForeignPaths = levelPivotGetForeignPaths;
    fdwroutine->GetForeignPlan = levelPivotGetForeignPlan;
    fdwroutine->BeginForeignScan = levelPivotBeginForeignScan;
    fdwroutine->IterateForeignScan = levelPivotIterateForeignScan;
    fdwroutine->ReScanForeignScan = levelPivotReScanForeignScan;
    fdwroutine->EndForeignScan = levelPivotEndForeignScan;

    /* Explain callback */
    fdwroutine->ExplainForeignScan = levelPivotExplainForeignScan;

    /* Modify callbacks */
    fdwroutine->AddForeignUpdateTargets = levelPivotAddForeignUpdateTargets;
    fdwroutine->PlanForeignModify = levelPivotPlanForeignModify;
    fdwroutine->BeginForeignModify = levelPivotBeginForeignModify;
    fdwroutine->ExecForeignInsert = levelPivotExecForeignInsert;
    fdwroutine->ExecForeignUpdate = levelPivotExecForeignUpdate;
    fdwroutine->ExecForeignDelete = levelPivotExecForeignDelete;
    fdwroutine->EndForeignModify = levelPivotEndForeignModify;
    fdwroutine->IsForeignRelUpdatable = levelPivotIsForeignRelUpdatable;

    /* Schema import callback */
    fdwroutine->ImportForeignSchema = levelPivotImportForeignSchema;

    PG_RETURN_POINTER(fdwroutine);
}

/**
 * FDW validator function
 *
 * Validates options for SERVER and FOREIGN TABLE.
 */
Datum
level_pivot_fdw_validator(PG_FUNCTION_ARGS)
{
    List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid catalog = PG_GETARG_OID(1);

    levelPivotValidateOptions(options_list, catalog);

    PG_RETURN_VOID();
}
