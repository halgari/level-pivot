/**
 * FDW callback implementations for level_pivot
 */

// PostgreSQL headers must come first for Windows compatibility
extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/table.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/explain_format.h"
#include "executor/executor.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/appendinfo.h"
#include "optimizer/optimizer.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "parser/parsetree.h"
#include "utils/builtins.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/syscache.h"
}

#include "level_pivot/key_pattern.hpp"
#include "level_pivot/key_parser.hpp"
#include "level_pivot/projection.hpp"
#include "level_pivot/pivot_scanner.hpp"
#include "level_pivot/connection_manager.hpp"
#include "level_pivot/type_converter.hpp"
#include "level_pivot/writer.hpp"
#include "level_pivot/schema_discovery.hpp"
#include "level_pivot/error.hpp"

#include <memory>
#include <string>
#include <cstring>

namespace {

/* Scan state structure */
struct LevelPivotScanState {
    std::unique_ptr<level_pivot::Projection> projection;
    std::unique_ptr<level_pivot::PivotScanner> scanner;
    std::shared_ptr<level_pivot::LevelDBConnection> connection;
    MemoryContext temp_context;
};

/* Modify state structure */
struct LevelPivotModifyState {
    std::unique_ptr<level_pivot::Projection> projection;
    std::unique_ptr<level_pivot::Writer> writer;
    std::shared_ptr<level_pivot::LevelDBConnection> connection;
    int num_cols;
    AttrNumber *attr_map;  // Maps foreign column attnums to local slot positions
};

/* Helper functions */

level_pivot::ConnectionOptions
get_server_options(ForeignServer *server)
{
    level_pivot::ConnectionOptions options;
    ListCell *cell;

    foreach(cell, server->options)
    {
        DefElem *def = (DefElem *) lfirst(cell);

        if (strcmp(def->defname, "db_path") == 0)
            options.db_path = defGetString(def);
        else if (strcmp(def->defname, "read_only") == 0)
            options.read_only = defGetBoolean(def);
        else if (strcmp(def->defname, "create_if_missing") == 0)
            options.create_if_missing = defGetBoolean(def);
        else if (strcmp(def->defname, "block_cache_size") == 0)
            options.block_cache_size = strtoul(defGetString(def), NULL, 10);
        else if (strcmp(def->defname, "write_buffer_size") == 0)
            options.write_buffer_size = strtoul(defGetString(def), NULL, 10);
    }

    return options;
}

std::string
get_table_option(ForeignTable *table, const char *name)
{
    ListCell *cell;

    foreach(cell, table->options)
    {
        DefElem *def = (DefElem *) lfirst(cell);
        if (strcmp(def->defname, name) == 0)
            return defGetString(def);
    }

    return "";
}

std::unique_ptr<level_pivot::Projection>
build_projection_from_relation(Relation rel, const std::string& key_pattern)
{
    level_pivot::KeyPattern pattern(key_pattern);
    TupleDesc tupdesc = RelationGetDescr(rel);

    std::vector<level_pivot::ColumnDef> columns;
    columns.reserve(tupdesc->natts);

    const auto& capture_names = pattern.capture_names();

    for (int i = 0; i < tupdesc->natts; i++)
    {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

        if (attr->attisdropped)
            continue;

        level_pivot::ColumnDef col;
        col.name = NameStr(attr->attname);
        col.type = level_pivot::pg_type_from_oid(attr->atttypid);
        col.attnum = attr->attnum;

        // Check if this is an identity column
        col.is_identity = false;
        for (const auto& cap_name : capture_names)
        {
            if (col.name == cap_name)
            {
                col.is_identity = true;
                break;
            }
        }

        columns.push_back(col);
    }

    return std::make_unique<level_pivot::Projection>(pattern, std::move(columns));
}

} // anonymous namespace

extern "C" {

/*
 * GetForeignRelSize
 *      Estimate the size of the foreign relation
 */
void
levelPivotGetForeignRelSize(PlannerInfo *root,
                            RelOptInfo *baserel,
                            Oid foreigntableid)
{
    /* For now, use a simple estimate */
    baserel->rows = 1000;
}

/*
 * GetForeignPaths
 *      Create possible access paths for the foreign table
 */
void
levelPivotGetForeignPaths(PlannerInfo *root,
                          RelOptInfo *baserel,
                          Oid foreigntableid)
{
    Cost startup_cost = 10;
    Cost total_cost = startup_cost + baserel->rows * 0.01;

    /* Create a single ForeignPath */
    add_path(baserel, (Path *)
             create_foreignscan_path(root, baserel,
                                    NULL,    /* default pathtarget */
                                    baserel->rows,
                                    0,       /* disabled_nodes */
                                    startup_cost,
                                    total_cost,
                                    NIL,     /* no pathkeys */
                                    baserel->lateral_relids,
                                    NULL,    /* no extra plan */
                                    NIL,     /* no fdw_restrictinfo */
                                    NIL));   /* no fdw_private */
}

/*
 * GetForeignPlan
 *      Create a ForeignScan plan node
 */
ForeignScan *
levelPivotGetForeignPlan(PlannerInfo *root,
                         RelOptInfo *baserel,
                         Oid foreigntableid,
                         ForeignPath *best_path,
                         List *tlist,
                         List *scan_clauses,
                         Plan *outer_plan)
{
    Index scan_relid = baserel->relid;

    /* Remove pseudoconstant clauses */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    return make_foreignscan(tlist,
                           scan_clauses,
                           scan_relid,
                           NIL,     /* no expressions to evaluate */
                           NIL,     /* no private data */
                           NIL,     /* no custom tlist */
                           NIL,     /* no remote quals */
                           outer_plan);
}

/*
 * BeginForeignScan
 *      Initialize the scan state
 */
void
levelPivotBeginForeignScan(ForeignScanState *node, int eflags)
{
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    PG_TRY_CPP({
        EState *estate = node->ss.ps.state;
        Relation rel = node->ss.ss_currentRelation;
        ForeignTable *table = GetForeignTable(RelationGetRelid(rel));
        ForeignServer *server = GetForeignServer(table->serverid);

        /* Get options */
        auto conn_options = get_server_options(server);
        std::string key_pattern = get_table_option(table, "key_pattern");

        /* Create scan state */
        auto state = new LevelPivotScanState();

        /* Build projection from table definition */
        state->projection = build_projection_from_relation(rel, key_pattern);

        /* Get connection */
        state->connection = level_pivot::ConnectionManager::instance()
            .get_connection(server->serverid, conn_options);

        /* Create scanner */
        state->scanner = std::make_unique<level_pivot::PivotScanner>(
            *state->projection, state->connection);

        /* Create temp memory context */
        state->temp_context = AllocSetContextCreate(estate->es_query_cxt,
                                                    "level_pivot temp",
                                                    ALLOCSET_DEFAULT_SIZES);

        /* Begin scan */
        state->scanner->begin_scan();

        node->fdw_state = state;
    });
}

/*
 * IterateForeignScan
 *      Fetch one row from the foreign table
 */
TupleTableSlot *
levelPivotIterateForeignScan(ForeignScanState *node)
{
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    auto state = static_cast<LevelPivotScanState *>(node->fdw_state);

    ExecClearTuple(slot);

    PG_TRY_CPP_RETURN({
        auto row = state->scanner->next_row();
        if (!row)
            return slot;

        /* Switch to temp context for value conversion */
        MemoryContext oldctx = MemoryContextSwitchTo(state->temp_context);

        /* Build tuple */
        TupleDesc tupdesc = slot->tts_tupleDescriptor;
        Datum *values = slot->tts_values;
        bool *nulls = slot->tts_isnull;

        /* Initialize all to NULL */
        memset(nulls, true, tupdesc->natts * sizeof(bool));

        /* Fill in values using DatumBuilder */
        level_pivot::DatumBuilder::build_datums(*row, *state->projection,
                                                 values, nulls);

        MemoryContextSwitchTo(oldctx);
        MemoryContextReset(state->temp_context);

        ExecStoreVirtualTuple(slot);
        return slot;
    }, slot);
}

/*
 * ReScanForeignScan
 *      Restart the scan from the beginning
 */
void
levelPivotReScanForeignScan(ForeignScanState *node)
{
    auto state = static_cast<LevelPivotScanState *>(node->fdw_state);

    PG_TRY_CPP({
        state->scanner->rescan();
    });
}

/*
 * EndForeignScan
 *      Clean up the scan state
 */
void
levelPivotEndForeignScan(ForeignScanState *node)
{
    auto state = static_cast<LevelPivotScanState *>(node->fdw_state);

    if (state)
    {
        state->scanner->end_scan();
        if (state->temp_context)
            MemoryContextDelete(state->temp_context);
        delete state;
        node->fdw_state = nullptr;
    }
}

/*
 * ExplainForeignScan
 *      Print additional EXPLAIN output
 */
void
levelPivotExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
    auto state = static_cast<LevelPivotScanState *>(node->fdw_state);

    if (state && state->scanner)
    {
        const auto& stats = state->scanner->stats();
        ExplainPropertyInteger("LevelDB Keys Scanned", NULL,
                              stats.keys_scanned, es);
        ExplainPropertyInteger("LevelDB Keys Skipped", NULL,
                              stats.keys_skipped, es);
        ExplainPropertyInteger("Rows Returned", NULL,
                              stats.rows_returned, es);
    }
}

/*
 * AddForeignUpdateTargets
 *      Add resjunk columns needed for UPDATE/DELETE
 */
void
levelPivotAddForeignUpdateTargets(PlannerInfo *root,
                                  Index rtindex,
                                  RangeTblEntry *target_rte,
                                  Relation target_relation)
{
    /* We use the whole row as the row identifier */
    Var *var = makeWholeRowVar(target_rte, rtindex, 0, false);
    add_row_identity_var(root, var, rtindex, "wholerow");
}

/*
 * PlanForeignModify
 *      Plan INSERT/UPDATE/DELETE operations
 */
List *
levelPivotPlanForeignModify(PlannerInfo *root,
                            ModifyTable *plan,
                            Index resultRelation,
                            int subplan_index)
{
    /* Return empty list - we don't need any private data */
    return NIL;
}

/*
 * BeginForeignModify
 *      Initialize the modify state
 */
void
levelPivotBeginForeignModify(ModifyTableState *mtstate,
                             ResultRelInfo *rinfo,
                             List *fdw_private,
                             int subplan_index,
                             int eflags)
{
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY)
        return;

    PG_TRY_CPP({
        Relation rel = rinfo->ri_RelationDesc;
        ForeignTable *table = GetForeignTable(RelationGetRelid(rel));
        ForeignServer *server = GetForeignServer(table->serverid);

        /* Get options */
        auto conn_options = get_server_options(server);
        conn_options.read_only = false;  /* Need write access */

        std::string key_pattern = get_table_option(table, "key_pattern");

        /* Create modify state */
        auto state = new LevelPivotModifyState();

        /* Build projection */
        state->projection = build_projection_from_relation(rel, key_pattern);

        /* Get connection */
        state->connection = level_pivot::ConnectionManager::instance()
            .get_connection(server->serverid, conn_options);

        /* Create writer */
        state->writer = std::make_unique<level_pivot::Writer>(
            *state->projection, state->connection);

        /* Store column count */
        TupleDesc tupdesc = RelationGetDescr(rel);
        state->num_cols = tupdesc->natts;

        rinfo->ri_FdwState = state;
    });
}

/*
 * ExecForeignInsert
 *      Insert a row into the foreign table
 */
TupleTableSlot *
levelPivotExecForeignInsert(EState *estate,
                            ResultRelInfo *rinfo,
                            TupleTableSlot *slot,
                            TupleTableSlot *planSlot)
{
    auto state = static_cast<LevelPivotModifyState *>(rinfo->ri_FdwState);

    PG_TRY_CPP_RETURN({
        slot_getallattrs(slot);
        state->writer->insert(slot->tts_values, slot->tts_isnull);
        return slot;
    }, slot);
}

/*
 * ExecForeignUpdate
 *      Update a row in the foreign table
 */
TupleTableSlot *
levelPivotExecForeignUpdate(EState *estate,
                            ResultRelInfo *rinfo,
                            TupleTableSlot *slot,
                            TupleTableSlot *planSlot)
{
    auto state = static_cast<LevelPivotModifyState *>(rinfo->ri_FdwState);

    PG_TRY_CPP_RETURN({
        /* Get the old row from the wholerow junk attribute */
        bool isnull;
        Datum datum = ExecGetJunkAttribute(planSlot,
                          rinfo->ri_RowIdAttNo,
                          &isnull);

        if (isnull)
            elog(ERROR, "wholerow is NULL");

        HeapTupleHeader oldtup = DatumGetHeapTupleHeader(datum);

        /* Extract old values */
        TupleDesc tupdesc = RelationGetDescr(rinfo->ri_RelationDesc);
        Datum *old_values = (Datum *) palloc(state->num_cols * sizeof(Datum));
        bool *old_nulls = (bool *) palloc(state->num_cols * sizeof(bool));

        for (int i = 0; i < state->num_cols; i++)
        {
            old_values[i] = GetAttributeByNum(oldtup, i + 1, &old_nulls[i]);
        }

        /* Get new values from slot */
        slot_getallattrs(slot);

        state->writer->update(old_values, old_nulls,
                             slot->tts_values, slot->tts_isnull);

        pfree(old_values);
        pfree(old_nulls);

        return slot;
    }, slot);
}

/*
 * ExecForeignDelete
 *      Delete a row from the foreign table
 */
TupleTableSlot *
levelPivotExecForeignDelete(EState *estate,
                            ResultRelInfo *rinfo,
                            TupleTableSlot *slot,
                            TupleTableSlot *planSlot)
{
    auto state = static_cast<LevelPivotModifyState *>(rinfo->ri_FdwState);

    PG_TRY_CPP_RETURN({
        /* Get the row from the wholerow junk attribute */
        bool isnull;
        Datum datum = ExecGetJunkAttribute(planSlot,
                          rinfo->ri_RowIdAttNo,
                          &isnull);

        if (isnull)
            elog(ERROR, "wholerow is NULL");

        HeapTupleHeader oldtup = DatumGetHeapTupleHeader(datum);

        /* Extract values */
        Datum *values = (Datum *) palloc(state->num_cols * sizeof(Datum));
        bool *nulls = (bool *) palloc(state->num_cols * sizeof(bool));

        for (int i = 0; i < state->num_cols; i++)
        {
            values[i] = GetAttributeByNum(oldtup, i + 1, &nulls[i]);
        }

        state->writer->remove(values, nulls);

        pfree(values);
        pfree(nulls);

        return slot;
    }, slot);
}

/*
 * EndForeignModify
 *      Clean up the modify state
 */
void
levelPivotEndForeignModify(EState *estate, ResultRelInfo *rinfo)
{
    auto state = static_cast<LevelPivotModifyState *>(rinfo->ri_FdwState);

    if (state)
    {
        delete state;
        rinfo->ri_FdwState = nullptr;
    }
}

/*
 * IsForeignRelUpdatable
 *      Check if the foreign table supports INSERT/UPDATE/DELETE
 */
int
levelPivotIsForeignRelUpdatable(Relation rel)
{
    ForeignTable *table = GetForeignTable(RelationGetRelid(rel));
    ForeignServer *server = GetForeignServer(table->serverid);

    /* Check if server is read-only */
    ListCell *cell;
    foreach(cell, server->options)
    {
        DefElem *def = (DefElem *) lfirst(cell);
        if (strcmp(def->defname, "read_only") == 0)
        {
            if (defGetBoolean(def))
                return 0;  /* Read-only, no modifications allowed */
        }
    }

    /* Default: allow all operations */
    return (1 << CMD_INSERT) | (1 << CMD_UPDATE) | (1 << CMD_DELETE);
}

/*
 * ImportForeignSchema
 *      Generate CREATE FOREIGN TABLE statements for discovered attrs
 */
List *
levelPivotImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
    List *commands = NIL;

    PG_TRY_CPP_RETURN({
        ForeignServer *server = GetForeignServer(serverOid);
        auto conn_options = get_server_options(server);

        /* Get connection */
        auto connection = level_pivot::ConnectionManager::instance()
            .get_connection(server->serverid, conn_options);

        /* Create schema discovery */
        level_pivot::SchemaDiscovery discovery(connection);

        /* Try to infer a pattern from the data */
        auto pattern_str = discovery.infer_pattern(1000);

        if (pattern_str)
        {
            level_pivot::KeyPattern pattern(*pattern_str);
            level_pivot::DiscoveryOptions opts;
            opts.max_keys = 10000;

            auto result = discovery.discover(pattern, opts);

            /* Generate SQL */
            std::string sql = level_pivot::generate_foreign_table_sql(
                stmt->remote_schema,
                server->servername,
                *pattern_str,
                result,
                true);

            commands = lappend(commands, makeString(pstrdup(sql.c_str())));
        }

        return commands;
    }, NIL);
}

} /* extern "C" */
