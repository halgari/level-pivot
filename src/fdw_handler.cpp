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
#include "catalog/pg_operator.h"
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
#include "level_pivot/pg_memory.hpp"

#include <memory>
#include <string>
#include <cstring>

namespace {

/* Type aliases to avoid macro comma issues with templates */
using DatumTempArray = level_pivot::TempArray<Datum, 64>;
using BoolTempArray = level_pivot::TempArray<bool, 64>;

/* Scan state structure */
struct LevelPivotScanState {
    std::unique_ptr<level_pivot::Projection> projection;
    std::unique_ptr<level_pivot::PivotScanner> scanner;
    std::shared_ptr<level_pivot::LevelDBConnection> connection;
    MemoryContext temp_context;
    bool cleaned_up;
    std::vector<std::string> prefix_values;  // Pushdown filter values

    LevelPivotScanState() : temp_context(nullptr), cleaned_up(false) {}

    ~LevelPivotScanState() { cleanup(); }

    void cleanup() {
        if (cleaned_up)
            return;
        cleaned_up = true;

        if (scanner)
            scanner->end_scan();
        scanner.reset();
        projection.reset();
        connection.reset();
        // Note: temp_context is a child of scan_ctx, will be deleted with parent
    }
};

/* Modify state structure */
struct LevelPivotModifyState {
    std::unique_ptr<level_pivot::Projection> projection;
    std::unique_ptr<level_pivot::Writer> writer;
    std::shared_ptr<level_pivot::LevelDBConnection> connection;
    int num_cols;
    AttrNumber *attr_map;  // Maps foreign column attnums to local slot positions
    bool use_write_batch;
    bool cleaned_up;

    LevelPivotModifyState() : num_cols(0), attr_map(nullptr), use_write_batch(true), cleaned_up(false) {}

    ~LevelPivotModifyState() { cleanup(); }

    void cleanup() {
        if (cleaned_up)
            return;
        cleaned_up = true;

        // Discard any uncommitted batch operations
        if (writer && writer->is_batched()) {
            writer->discard_batch();
        }
        writer.reset();
        projection.reset();
        connection.reset();
    }
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
        else if (strcmp(def->defname, "use_write_batch") == 0)
            options.use_write_batch = defGetBoolean(def);
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

/**
 * Parse fdw_private list and build prefix values for filter pushdown.
 *
 * @param fdw_private List of (attnum, value) pairs from GetForeignPlan
 * @param projection Table projection to get identity column order
 * @return Vector of prefix values in identity column order
 */
static std::vector<std::string>
build_prefix_from_fdw_private(List *fdw_private,
                              const level_pivot::Projection& projection)
{
    /* Parse fdw_private into attnum->value map */
    std::unordered_map<AttrNumber, std::string> filter_values;

    ListCell *lc = list_head(fdw_private);
    while (lc != NULL)
    {
        AttrNumber attnum = intVal(lfirst(lc));
        lc = lnext(fdw_private, lc);
        if (lc == NULL)
            break;
        char *value = strVal(lfirst(lc));
        lc = lnext(fdw_private, lc);
        filter_values[attnum] = std::string(value);
    }

    /*
     * Build prefix_values in identity column order.
     * Only include leading consecutive columns - stop at first missing one.
     */
    std::vector<std::string> prefix_values;
    const auto& identity_cols = projection.identity_columns();

    for (const auto* col : identity_cols)
    {
        auto it = filter_values.find(col->attnum);
        if (it != filter_values.end())
            prefix_values.push_back(it->second);
        else
            break;  /* Stop at first missing identity column */
    }

    return prefix_values;
}

/**
 * Check if a clause is a pushable equality condition on an identity column.
 *
 * For filter pushdown, we only support simple "column = constant" expressions
 * where the column is an identity column (part of the key pattern).
 *
 * @param clause The expression to check
 * @param baserel The relation being scanned
 * @param identity_attnums Attribute numbers of identity columns
 * @param out_attnum Output: the attribute number of the matched column
 * @param out_value Output: the constant value (must be pfree'd by caller)
 * @return true if this is a pushable equality condition
 */
static bool
is_pushable_equality(Expr *clause, RelOptInfo *baserel,
                     const std::vector<AttrNumber>& identity_attnums,
                     AttrNumber *out_attnum, char **out_value)
{
    /* Must be an OpExpr */
    if (!IsA(clause, OpExpr))
        return false;

    OpExpr *op = (OpExpr *) clause;

    /* Must have exactly 2 arguments */
    if (list_length(op->args) != 2)
        return false;

    /* Check for equality operator by looking up the operator */
    HeapTuple opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(op->opno));
    if (!HeapTupleIsValid(opertup))
        return false;

    Form_pg_operator operform = (Form_pg_operator) GETSTRUCT(opertup);
    bool is_equality = (operform->oprresult == BOOLOID &&
                        strcmp(NameStr(operform->oprname), "=") == 0);
    ReleaseSysCache(opertup);

    if (!is_equality)
        return false;

    Expr *left = (Expr *) linitial(op->args);
    Expr *right = (Expr *) lsecond(op->args);

    /* Look for Var = Const pattern (either direction) */
    Var *var = NULL;
    Const *constval = NULL;

    if (IsA(left, Var) && IsA(right, Const)) {
        var = (Var *) left;
        constval = (Const *) right;
    } else if (IsA(left, Const) && IsA(right, Var)) {
        var = (Var *) right;
        constval = (Const *) left;
    } else {
        return false;
    }

    /* Check if var is from this relation */
    if (var->varno != baserel->relid)
        return false;

    /* Check if this is an identity column */
    bool is_identity = false;
    for (AttrNumber id_attnum : identity_attnums) {
        if (var->varattno == id_attnum) {
            is_identity = true;
            break;
        }
    }
    if (!is_identity)
        return false;

    /* Must not be NULL */
    if (constval->constisnull)
        return false;

    /* Convert Datum to string based on type */
    char *value = NULL;
    Oid typid = constval->consttype;

    if (typid == TEXTOID || typid == VARCHAROID || typid == BPCHAROID) {
        value = TextDatumGetCString(constval->constvalue);
    } else if (typid == INT4OID) {
        int32 intval = DatumGetInt32(constval->constvalue);
        value = psprintf("%d", intval);
    } else if (typid == INT8OID) {
        int64 intval = DatumGetInt64(constval->constvalue);
        value = psprintf("%ld", (long) intval);
    } else {
        /* For other types, use output function */
        Oid typoutput;
        bool typIsVarlena;
        getTypeOutputInfo(typid, &typoutput, &typIsVarlena);
        value = OidOutputFunctionCall(typoutput, constval->constvalue);
    }

    *out_attnum = var->varattno;
    *out_value = value;
    return true;
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
 *
 * Extracts equality conditions on identity columns for filter pushdown.
 * The fdw_private list stores (attnum, value) pairs for pushable filters.
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
    List *fdw_private = NIL;

    /*
     * Get the key pattern to determine identity columns.
     * We need to know which columns are identity columns to determine
     * which filters can be pushed down.
     */
    ForeignTable *table = GetForeignTable(foreigntableid);
    std::string key_pattern = get_table_option(table, "key_pattern");

    if (!key_pattern.empty()) {
        /* Parse the key pattern to get capture names */
        level_pivot::KeyPattern pattern(key_pattern);
        const auto& capture_names = pattern.capture_names();

        /* Build a map from column name to attnum for identity columns */
        RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);
        Relation rel = table_open(rte->relid, NoLock);
        TupleDesc tupdesc = RelationGetDescr(rel);

        /* Collect attnums of identity columns */
        std::vector<AttrNumber> identity_attnums;
        for (int i = 0; i < tupdesc->natts; i++) {
            Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
            if (attr->attisdropped)
                continue;

            std::string col_name = NameStr(attr->attname);
            for (const auto& cap_name : capture_names) {
                if (col_name == cap_name) {
                    identity_attnums.push_back(attr->attnum);
                    break;
                }
            }
        }
        table_close(rel, NoLock);

        /* Extract pushable equality conditions from scan_clauses */
        ListCell *cell;
        foreach(cell, scan_clauses) {
            RestrictInfo *rinfo = lfirst_node(RestrictInfo, cell);
            Expr *clause = rinfo->clause;

            AttrNumber attnum;
            char *value;
            if (is_pushable_equality(clause, baserel, identity_attnums,
                                     &attnum, &value)) {
                /* Store (attnum, value) pair in fdw_private */
                fdw_private = lappend(fdw_private, makeInteger(attnum));
                fdw_private = lappend(fdw_private, makeString(pstrdup(value)));
            }
        }
    }

    /* Remove pseudoconstant clauses - all clauses still checked by PostgreSQL */
    scan_clauses = extract_actual_clauses(scan_clauses, false);

    return make_foreignscan(tlist,
                           scan_clauses,
                           scan_relid,
                           NIL,         /* no expressions to evaluate */
                           fdw_private, /* pushed filter info */
                           NIL,         /* no custom tlist */
                           NIL,         /* no remote quals */
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
        ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;

        /* Get options */
        auto conn_options = get_server_options(server);
        std::string key_pattern = get_table_option(table, "key_pattern");

        /* Create dedicated memory context for scan state */
        MemoryContext scan_ctx = AllocSetContextCreate(estate->es_query_cxt,
                                                       "level_pivot scan",
                                                       ALLOCSET_DEFAULT_SIZES);

        /* Create scan state using pg_construct for automatic cleanup */
        auto state = level_pivot::pg_construct<LevelPivotScanState>(scan_ctx);

        /* Build projection from table definition */
        state->projection = build_projection_from_relation(rel, key_pattern);

        /* Get connection */
        state->connection = level_pivot::ConnectionManager::instance()
            .get_connection(server->serverid, conn_options);

        /* Create scanner */
        state->scanner = std::make_unique<level_pivot::PivotScanner>(
            *state->projection, state->connection);

        /* Create temp memory context as child of scan context */
        state->temp_context = AllocSetContextCreate(scan_ctx,
                                                    "level_pivot temp",
                                                    ALLOCSET_DEFAULT_SIZES);

        /* Build prefix values from fdw_private for filter pushdown */
        state->prefix_values = build_prefix_from_fdw_private(
            fsplan->fdw_private, *state->projection);

        /* Begin scan with prefix filter */
        state->scanner->begin_scan(state->prefix_values);

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
        /* Rescan with the same prefix values for filter pushdown */
        state->scanner->begin_scan(state->prefix_values);
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
        /* Call cleanup() to release resources early.
         * The destructor will also be called via memory context callback
         * when the scan context is deleted, but cleanup() is idempotent. */
        state->cleanup();
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

    /* Show pushed filters even without ANALYZE */
    ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;
    if (fsplan->fdw_private != NIL) {
        /*
         * Build a description of pushed filters from fdw_private.
         * Need to map attnums back to column names.
         */
        Relation rel = node->ss.ss_currentRelation;
        TupleDesc tupdesc = RelationGetDescr(rel);

        std::string filters;
        List *fdw_private = fsplan->fdw_private;
        ListCell *cell = list_head(fdw_private);

        while (cell != NULL) {
            AttrNumber attnum = intVal(lfirst(cell));
            cell = lnext(fdw_private, cell);
            if (cell == NULL)
                break;
            char *value = strVal(lfirst(cell));
            cell = lnext(fdw_private, cell);

            /* Find column name for this attnum */
            const char *colname = NULL;
            for (int i = 0; i < tupdesc->natts; i++) {
                Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
                if (attr->attnum == attnum) {
                    colname = NameStr(attr->attname);
                    break;
                }
            }

            if (colname) {
                if (!filters.empty())
                    filters += ", ";
                filters += colname;
                filters += "='";
                filters += value;
                filters += "'";
            }
        }

        if (!filters.empty()) {
            ExplainPropertyText("LevelDB Prefix Filter", filters.c_str(), es);
        }
    }

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
        EState *estate = mtstate->ps.state;
        Relation rel = rinfo->ri_RelationDesc;
        ForeignTable *table = GetForeignTable(RelationGetRelid(rel));
        ForeignServer *server = GetForeignServer(table->serverid);

        /* Get options */
        auto conn_options = get_server_options(server);
        conn_options.read_only = false;  /* Need write access */

        std::string key_pattern = get_table_option(table, "key_pattern");

        /* Create dedicated memory context for modify state */
        MemoryContext modify_ctx = AllocSetContextCreate(estate->es_query_cxt,
                                                         "level_pivot modify",
                                                         ALLOCSET_DEFAULT_SIZES);

        /* Create modify state using pg_construct for automatic cleanup */
        auto state = level_pivot::pg_construct<LevelPivotModifyState>(modify_ctx);

        /* Build projection */
        state->projection = build_projection_from_relation(rel, key_pattern);

        /* Get connection */
        state->connection = level_pivot::ConnectionManager::instance()
            .get_connection(server->serverid, conn_options);

        /* Store write batch setting */
        state->use_write_batch = conn_options.use_write_batch;

        /* Create writer - with or without batch depending on options */
        if (conn_options.use_write_batch) {
            auto batch = std::make_unique<level_pivot::LevelDBWriteBatch>(
                state->connection->create_batch());
            state->writer = std::make_unique<level_pivot::Writer>(
                *state->projection, state->connection, std::move(batch));
        } else {
            state->writer = std::make_unique<level_pivot::Writer>(
                *state->projection, state->connection);
        }

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

        /* Extract old values using stack-based TempArray */
        DatumTempArray old_values(state->num_cols);
        BoolTempArray old_nulls(state->num_cols);

        for (int i = 0; i < state->num_cols; i++)
        {
            old_values[i] = GetAttributeByNum(oldtup, i + 1, &old_nulls[i]);
        }

        /* Get new values from slot */
        slot_getallattrs(slot);

        state->writer->update(old_values.data(), old_nulls.data(),
                             slot->tts_values, slot->tts_isnull);

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

        /* Extract values using stack-based TempArray */
        DatumTempArray values(state->num_cols);
        BoolTempArray nulls(state->num_cols);

        for (int i = 0; i < state->num_cols; i++)
        {
            values[i] = GetAttributeByNum(oldtup, i + 1, &nulls[i]);
        }

        state->writer->remove(values.data(), nulls.data());

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
        PG_TRY_CPP({
            /* Commit batch if using batched writes */
            if (state->writer && state->use_write_batch) {
                state->writer->commit_batch();
            }
        });

        /* Call cleanup() to release resources early.
         * The destructor will also be called via memory context callback
         * when the modify context is deleted, but cleanup() is idempotent. */
        state->cleanup();
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
