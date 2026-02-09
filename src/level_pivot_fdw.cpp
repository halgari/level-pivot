/**
 * level_pivot_fdw.cpp - PostgreSQL Foreign Data Wrapper entry point
 *
 * This is the main entry point for the level_pivot extension. It:
 *   1. Registers the extension with PG_MODULE_MAGIC
 *   2. Exports handler and validator functions via PG_FUNCTION_INFO_V1
 *   3. Wires up the FdwRoutine with all callback implementations
 *
 * The FdwRoutine structure tells PostgreSQL which functions to call for:
 *   - Planning: GetForeignRelSize, GetForeignPaths, GetForeignPlan
 *   - Scanning: BeginForeignScan, IterateForeignScan, EndForeignScan
 *   - Modifying: BeginForeignModify, ExecForeignInsert/Update/Delete
 *   - Schema import: ImportForeignSchema
 *
 * Actual implementations are in fdw_handler.cpp to keep this file focused
 * on the PostgreSQL integration plumbing.
 */

// PostgreSQL headers must come first for Windows compatibility
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
 * FDW handler function - called by PostgreSQL to get our callback table.
 *
 * This is invoked when a query touches a level_pivot foreign table.
 * PostgreSQL uses the returned FdwRoutine to dispatch operations.
 *
 * The routine is allocated in the current memory context and returned
 * as a Datum (PostgreSQL's universal value type).
 */
Datum
level_pivot_fdw_handler(PG_FUNCTION_ARGS)
{
    FdwRoutine *fdwroutine = makeNode(FdwRoutine);

    /* Planning phase: estimate costs and build access paths */
    fdwroutine->GetForeignRelSize = levelPivotGetForeignRelSize;
    fdwroutine->GetForeignPaths = levelPivotGetForeignPaths;
    fdwroutine->GetForeignPlan = levelPivotGetForeignPlan;

    /* Scan execution: iterate through rows from LevelDB */
    fdwroutine->BeginForeignScan = levelPivotBeginForeignScan;
    fdwroutine->IterateForeignScan = levelPivotIterateForeignScan;
    fdwroutine->ReScanForeignScan = levelPivotReScanForeignScan;
    fdwroutine->EndForeignScan = levelPivotEndForeignScan;

    /* EXPLAIN output enhancement */
    fdwroutine->ExplainForeignScan = levelPivotExplainForeignScan;

    /* DML operations: INSERT, UPDATE, DELETE */
    fdwroutine->AddForeignUpdateTargets = levelPivotAddForeignUpdateTargets;
    fdwroutine->PlanForeignModify = levelPivotPlanForeignModify;
    fdwroutine->BeginForeignModify = levelPivotBeginForeignModify;
    fdwroutine->ExecForeignInsert = levelPivotExecForeignInsert;
    fdwroutine->ExecForeignUpdate = levelPivotExecForeignUpdate;
    fdwroutine->ExecForeignDelete = levelPivotExecForeignDelete;
    fdwroutine->EndForeignModify = levelPivotEndForeignModify;
    fdwroutine->IsForeignRelUpdatable = levelPivotIsForeignRelUpdatable;

    /* IMPORT FOREIGN SCHEMA support for auto-discovery */
    fdwroutine->ImportForeignSchema = levelPivotImportForeignSchema;

    PG_RETURN_POINTER(fdwroutine);
}

/**
 * FDW validator function - called during CREATE/ALTER SERVER/TABLE.
 *
 * PostgreSQL passes options as a packed Datum that we unpack into a List.
 * The catalog OID tells us whether we're validating server or table options.
 *
 * Validation happens before the DDL is committed, so invalid options
 * cause the statement to fail with a helpful error message.
 */
Datum
level_pivot_fdw_validator(PG_FUNCTION_ARGS)
{
    List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
    Oid catalog = PG_GETARG_OID(1);

    levelPivotValidateOptions(options_list, catalog);

    PG_RETURN_VOID();
}
