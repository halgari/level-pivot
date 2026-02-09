/**
 * fdw_handler.cpp - Core FDW callback implementations
 *
 * This is the main implementation file for the level_pivot FDW. It handles
 * the full query lifecycle for both pivot and raw table modes:
 *
 * QUERY PLANNING (GetForeignRelSize, GetForeignPaths, GetForeignPlan):
 *   - Estimates row counts for cost-based optimization
 *   - Creates access paths (currently just sequential scan)
 *   - Extracts pushable WHERE clauses (identity column equalities)
 *
 * SCAN EXECUTION (BeginForeignScan, IterateForeignScan, EndForeignScan):
 *   - BeginForeignScan: Opens LevelDB connection, creates scanner
 *   - IterateForeignScan: Returns rows one at a time
 *   - EndForeignScan: Closes scanner, releases resources
 *
 * DML EXECUTION (BeginForeignModify, ExecForeignInsert/Update/Delete):
 *   - Translates SQL DML to LevelDB put/delete operations
 *   - Uses WriteBatch for atomicity when configured
 *   - Sends NOTIFY on table modification
 *
 * SCHEMA IMPORT (ImportForeignSchema):
 *   - Analyzes existing LevelDB data to infer table structure
 *   - Generates CREATE FOREIGN TABLE statements
 *
 * The code branches on TableMode (PIVOT vs RAW) in most callbacks because
 * the two modes have fundamentally different data models.
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
#include "commands/async.h"  // For Async_Notify
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
#include "level_pivot/raw_scanner.hpp"
#include "level_pivot/raw_writer.hpp"
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

/* Type aliases for stack-allocated arrays used in DML.
 * TempArray avoids heap allocation for tables with < 64 columns. */
using DatumTempArray = level_pivot::TempArray<Datum, 64>;
using BoolTempArray = level_pivot::TempArray<bool, 64>;

/* Table mode determines the data access pattern:
 * PIVOT: Keys are parsed by pattern, multiple keys become one row
 * RAW: Direct key-value access, each key is one row */
enum class TableMode { PIVOT, RAW };

TableMode get_table_mode(ForeignTable *table)
{
    ListCell *cell;
    foreach(cell, table->options)
    {
        DefElem *def = (DefElem *) lfirst(cell);
        if (strcmp(def->defname, "table_mode") == 0)
        {
            std::string mode(defGetString(def));
            if (mode == "raw")
                return TableMode::RAW;
        }
    }
    return TableMode::PIVOT;  /* Default */
}

/**
 * Base class for FDW state structures.
 *
 * FDW state is allocated in a PostgreSQL MemoryContext and stored in
 * node->fdw_state. It persists across IterateForeignScan calls.
 *
 * The cleanup pattern handles PostgreSQL's requirement that cleanup
 * happens exactly once, even if EndForeignScan is called multiple times.
 */
struct FdwStateBase {
    std::shared_ptr<level_pivot::LevelDBConnection> connection;
    bool cleaned_up;

    FdwStateBase() : cleaned_up(false) {}
    virtual ~FdwStateBase() = default;

protected:
    /* Returns false if already cleaned up (idempotent cleanup) */
    bool begin_cleanup() {
        if (cleaned_up)
            return false;
        cleaned_up = true;
        return true;
    }

    void cleanup_connection() {
        connection.reset();
    }
};

/* Base struct for scan state (adds temp_context) */
struct ScanStateBase : FdwStateBase {
    MemoryContext temp_context;

    ScanStateBase() : temp_context(nullptr) {}
};

/* Base struct for modify state (adds NOTIFY support) */
struct ModifyStateBase : FdwStateBase {
    std::string schema_name;
    std::string table_name;
    bool has_modifications;
    bool use_write_batch;

    ModifyStateBase() : has_modifications(false), use_write_batch(true) {}
};

/* Scan state structure */
struct LevelPivotScanState : ScanStateBase {
    std::unique_ptr<level_pivot::Projection> projection;
    std::unique_ptr<level_pivot::PivotScanner> scanner;
    std::vector<std::string> prefix_values;  // Pushdown filter values

    ~LevelPivotScanState() { cleanup(); }

    void cleanup() {
        if (!begin_cleanup())
            return;

        if (scanner)
            scanner->end_scan();
        scanner.reset();
        projection.reset();
        cleanup_connection();
        // Note: temp_context is a child of scan_ctx, will be deleted with parent
    }
};

/* Raw scan state structure for raw table mode */
struct RawScanState : ScanStateBase {
    std::unique_ptr<level_pivot::RawScanner> scanner;
    level_pivot::RawScanBounds bounds;

    ~RawScanState() { cleanup(); }

    void cleanup() {
        if (!begin_cleanup())
            return;

        if (scanner)
            scanner->end_scan();
        scanner.reset();
        cleanup_connection();
    }
};

/* Modify state structure */
struct LevelPivotModifyState : ModifyStateBase {
    std::unique_ptr<level_pivot::Projection> projection;
    std::unique_ptr<level_pivot::Writer> writer;
    int num_cols;
    AttrNumber *attr_map;  // Maps foreign column attnums to local slot positions

    LevelPivotModifyState() : num_cols(0), attr_map(nullptr) {}

    ~LevelPivotModifyState() { cleanup(); }

    void cleanup() {
        if (!begin_cleanup())
            return;

        // Discard any uncommitted batch operations
        if (writer && writer->is_batched()) {
            writer->discard_batch();
        }
        writer.reset();
        projection.reset();
        cleanup_connection();
    }
};

/* Raw modify state structure for raw table mode */
struct RawModifyState : ModifyStateBase {
    std::unique_ptr<level_pivot::RawWriter> writer;
    AttrNumber key_attnum;    // Attribute number of the 'key' column
    AttrNumber value_attnum;  // Attribute number of the 'value' column

    RawModifyState() : key_attnum(0), value_attnum(0) {}

    ~RawModifyState() { cleanup(); }

    void cleanup() {
        if (!begin_cleanup())
            return;

        if (writer && writer->is_batched()) {
            writer->discard_batch();
        }
        writer.reset();
        cleanup_connection();
    }
};

/**
 * Builds NOTIFY channel name from schema and table.
 *
 * Format: {schema}_{table}_changed
 * This allows clients to LISTEN for changes to specific tables.
 * PostgreSQL limits channel names to 63 characters.
 */
static std::string
build_notify_channel(const std::string& schema_name, const std::string& table_name)
{
    std::string channel = schema_name + "_" + table_name + "_changed";

    if (channel.length() > 63)
        channel = channel.substr(0, 63);

    return channel;
}

/* Helper function for NOTIFY support */
static void
send_table_changed_notify(const std::string& schema_name, const std::string& table_name)
{
    std::string channel = build_notify_channel(schema_name, table_name);
    Async_Notify(channel.c_str(), NULL);
}

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

/**
 * Extract raw key predicate from a comparison clause.
 *
 * For raw tables, we support predicates on the 'key' column:
 *   - key = 'value' (exact match)
 *   - key > 'value', key >= 'value' (lower bound)
 *   - key < 'value', key <= 'value' (upper bound)
 *
 * @param clause The expression to check
 * @param baserel The relation being scanned
 * @param key_attnum Attribute number of the 'key' column
 * @param strategy Output: BTLessStrategyNumber, BTLessEqualStrategyNumber,
 *                 BTEqualStrategyNumber, BTGreaterEqualStrategyNumber, or BTGreaterStrategyNumber
 * @param value Output: the constant value (must be pfree'd by caller)
 * @return true if this is a pushable key predicate
 */
static bool
extract_raw_key_predicate(Expr *clause, RelOptInfo *baserel,
                          AttrNumber key_attnum,
                          int *strategy, char **value)
{
    /* Must be an OpExpr */
    if (!IsA(clause, OpExpr))
        return false;

    OpExpr *op = (OpExpr *) clause;

    /* Must have exactly 2 arguments */
    if (list_length(op->args) != 2)
        return false;

    /* Look up the operator */
    HeapTuple opertup = SearchSysCache1(OPEROID, ObjectIdGetDatum(op->opno));
    if (!HeapTupleIsValid(opertup))
        return false;

    Form_pg_operator operform = (Form_pg_operator) GETSTRUCT(opertup);

    /* Check if result is boolean */
    if (operform->oprresult != BOOLOID) {
        ReleaseSysCache(opertup);
        return false;
    }

    /* Determine strategy from operator name */
    const char *oprname = NameStr(operform->oprname);
    int strat = 0;
    bool swap_operands = false;

    if (strcmp(oprname, "=") == 0)
        strat = BTEqualStrategyNumber;
    else if (strcmp(oprname, "<") == 0)
        strat = BTLessStrategyNumber;
    else if (strcmp(oprname, "<=") == 0)
        strat = BTLessEqualStrategyNumber;
    else if (strcmp(oprname, ">") == 0)
        strat = BTGreaterStrategyNumber;
    else if (strcmp(oprname, ">=") == 0)
        strat = BTGreaterEqualStrategyNumber;
    else {
        ReleaseSysCache(opertup);
        return false;
    }

    ReleaseSysCache(opertup);

    Expr *left = (Expr *) linitial(op->args);
    Expr *right = (Expr *) lsecond(op->args);

    /* Look for Var op Const pattern (either direction) */
    Var *var = NULL;
    Const *constval = NULL;

    if (IsA(left, Var) && IsA(right, Const)) {
        var = (Var *) left;
        constval = (Const *) right;
    } else if (IsA(left, Const) && IsA(right, Var)) {
        var = (Var *) right;
        constval = (Const *) left;
        swap_operands = true;
    } else {
        return false;
    }

    /* Check if var is from this relation and is the key column */
    if (var->varno != baserel->relid || var->varattno != key_attnum)
        return false;

    /* Must not be NULL */
    if (constval->constisnull)
        return false;

    /* If operands were swapped, flip the comparison direction */
    if (swap_operands) {
        switch (strat) {
            case BTLessStrategyNumber:
                strat = BTGreaterStrategyNumber;
                break;
            case BTLessEqualStrategyNumber:
                strat = BTGreaterEqualStrategyNumber;
                break;
            case BTGreaterStrategyNumber:
                strat = BTLessStrategyNumber;
                break;
            case BTGreaterEqualStrategyNumber:
                strat = BTLessEqualStrategyNumber;
                break;
            /* BTEqualStrategyNumber stays the same */
        }
    }

    /* Convert Datum to string */
    char *val = NULL;
    Oid typid = constval->consttype;

    if (typid == TEXTOID || typid == VARCHAROID || typid == BPCHAROID) {
        val = TextDatumGetCString(constval->constvalue);
    } else {
        /* For other types, use output function */
        Oid typoutput;
        bool typIsVarlena;
        getTypeOutputInfo(typid, &typoutput, &typIsVarlena);
        val = OidOutputFunctionCall(typoutput, constval->constvalue);
    }

    *strategy = strat;
    *value = val;
    return true;
}

/**
 * Build RawScanBounds from fdw_private list.
 *
 * fdw_private format for raw tables:
 *   - First element: Integer marker (-1 for raw mode)
 *   - Subsequent pairs: (strategy, value) for each predicate
 */
static level_pivot::RawScanBounds
build_raw_bounds_from_fdw_private(List *fdw_private)
{
    level_pivot::RawScanBounds bounds;

    if (fdw_private == NIL)
        return bounds;

    ListCell *cell = list_head(fdw_private);

    /* Skip the mode marker */
    if (cell != NULL) {
        int marker = intVal(lfirst(cell));
        if (marker != -1)
            return bounds;  /* Not a raw mode private list */
        cell = lnext(fdw_private, cell);
    }

    /* Process (strategy, value) pairs */
    while (cell != NULL)
    {
        int strategy = intVal(lfirst(cell));
        cell = lnext(fdw_private, cell);
        if (cell == NULL)
            break;
        char *value = strVal(lfirst(cell));
        cell = lnext(fdw_private, cell);

        switch (strategy) {
            case BTEqualStrategyNumber:
                bounds.exact_key = std::string(value);
                break;
            case BTLessStrategyNumber:
                bounds.upper_bound = std::string(value);
                bounds.upper_inclusive = false;
                break;
            case BTLessEqualStrategyNumber:
                bounds.upper_bound = std::string(value);
                bounds.upper_inclusive = true;
                break;
            case BTGreaterStrategyNumber:
                bounds.lower_bound = std::string(value);
                bounds.lower_inclusive = false;
                break;
            case BTGreaterEqualStrategyNumber:
                bounds.lower_bound = std::string(value);
                bounds.lower_inclusive = true;
                break;
        }
    }

    return bounds;
}

/**
 * Find the attribute number of a column by name in a relation.
 */
static AttrNumber
find_column_attnum(Relation rel, const char *name)
{
    TupleDesc tupdesc = RelationGetDescr(rel);
    for (int i = 0; i < tupdesc->natts; i++) {
        Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
        if (attr->attisdropped)
            continue;
        if (strcmp(NameStr(attr->attname), name) == 0)
            return attr->attnum;
    }
    return InvalidAttrNumber;
}

} // anonymous namespace

extern "C" {

/*
 * GetForeignRelSize - Estimate row count for query planning.
 *
 * PostgreSQL uses this for cost estimation. A more accurate estimate
 * would sample LevelDB, but 1000 is a reasonable default that doesn't
 * add overhead. The actual row count doesn't affect correctness.
 */
void
levelPivotGetForeignRelSize(PlannerInfo *root,
                            RelOptInfo *baserel,
                            Oid foreigntableid)
{
    baserel->rows = 1000;  /* TODO: could sample for better estimates */
}

/*
 * GetForeignPaths - Create access path for the foreign table.
 *
 * Currently we only support sequential scan. Future optimization could add:
 *   - Index path when filtering on identity columns (uses LevelDB prefix seek)
 *   - Parameterized paths for nested loop joins
 *
 * Cost model: startup_cost + (rows * per_row_cost)
 * The per_row_cost (0.01) is a rough estimate for LevelDB iteration.
 */
void
levelPivotGetForeignPaths(PlannerInfo *root,
                          RelOptInfo *baserel,
                          Oid foreigntableid)
{
    Cost startup_cost = 10;
    Cost total_cost = startup_cost + baserel->rows * 0.01;

    add_path(baserel, (Path *)
             create_foreignscan_path(root, baserel,
                                    NULL,    /* default pathtarget */
                                    baserel->rows,
                                    0,       /* disabled_nodes */
                                    startup_cost,
                                    total_cost,
                                    NIL,     /* no pathkeys (unsorted) */
                                    baserel->lateral_relids,
                                    NULL,    /* no extra plan */
                                    NIL,     /* no fdw_restrictinfo */
                                    NIL));   /* no fdw_private yet */
}

/*
 * GetForeignPlan - Build the final scan plan with pushed-down predicates.
 *
 * This is where filter pushdown happens. We scan WHERE clauses looking for:
 *   - Pivot mode: "identity_column = constant" (uses LevelDB prefix seek)
 *   - Raw mode: "key op constant" where op is =, <, <=, >, >= (uses seek + bounds)
 *
 * Pushed predicates are stored in fdw_private for use by BeginForeignScan:
 *   - Pivot mode: [(attnum, value), ...] pairs
 *   - Raw mode: [-1, (strategy, value), ...] with BTStrategy constants
 *
 * Non-pushable predicates remain in scan_clauses for PostgreSQL to evaluate.
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

    ForeignTable *table = GetForeignTable(foreigntableid);
    TableMode mode = get_table_mode(table);

    if (mode == TableMode::RAW) {
        /* Raw mode: extract key predicates */
        RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);
        Relation rel = table_open(rte->relid, NoLock);

        /* Find the 'key' column */
        AttrNumber key_attnum = find_column_attnum(rel, "key");
        table_close(rel, NoLock);

        if (key_attnum != InvalidAttrNumber) {
            /* Mark this as raw mode with -1 */
            fdw_private = lappend(fdw_private, makeInteger(-1));

            /* Extract key predicates from scan_clauses */
            ListCell *cell;
            foreach(cell, scan_clauses) {
                RestrictInfo *rinfo = lfirst_node(RestrictInfo, cell);
                Expr *clause = rinfo->clause;

                int strategy;
                char *value;
                if (extract_raw_key_predicate(clause, baserel, key_attnum,
                                              &strategy, &value)) {
                    fdw_private = lappend(fdw_private, makeInteger(strategy));
                    fdw_private = lappend(fdw_private, makeString(pstrdup(value)));
                }
            }
        }
    } else {
        /* Pivot mode: extract identity column filters */
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
 * BeginForeignScan - Initialize scan state before iteration.
 *
 * Creates scanner (PivotScanner or RawScanner) and opens LevelDB connection.
 * The scanner is positioned using pushed-down predicates from fdw_private.
 *
 * Memory management: We create a dedicated MemoryContext for scan state
 * that lives until EndForeignScan. A child "temp" context is reset after
 * each row to avoid memory growth during large scans.
 */
void
levelPivotBeginForeignScan(ForeignScanState *node, int eflags)
{
    /* EXPLAIN ANALYZE still calls this but doesn't iterate */
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
        TableMode mode = get_table_mode(table);

        /* Create dedicated memory context for scan state */
        MemoryContext scan_ctx = AllocSetContextCreate(estate->es_query_cxt,
                                                       "level_pivot scan",
                                                       ALLOCSET_DEFAULT_SIZES);

        if (mode == TableMode::RAW) {
            /* Raw mode: use RawScanner */
            auto state = level_pivot::pg_construct<RawScanState>(scan_ctx);

            /* Get connection */
            state->connection = level_pivot::ConnectionManager::instance()
                .get_connection(server->serverid, conn_options);

            /* Create scanner */
            state->scanner = std::make_unique<level_pivot::RawScanner>(
                state->connection);

            /* Create temp memory context */
            state->temp_context = AllocSetContextCreate(scan_ctx,
                                                        "level_pivot temp",
                                                        ALLOCSET_DEFAULT_SIZES);

            /* Build bounds from fdw_private */
            state->bounds = build_raw_bounds_from_fdw_private(fsplan->fdw_private);

            /* Begin scan with bounds */
            state->scanner->begin_scan(state->bounds);

            node->fdw_state = state;
        } else {
            /* Pivot mode: use PivotScanner */
            std::string key_pattern = get_table_option(table, "key_pattern");

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
        }
    });
}

/*
 * IterateForeignScan - Return the next row, or empty slot if done.
 *
 * This is the hot path - called once per row. Key optimizations:
 *   - Uses temp_context that's reset per row (avoids palloc accumulation)
 *   - PivotScanner uses zero-copy string_views until row is complete
 *   - DatumBuilder pre-computes column index mappings
 *
 * Returns an empty slot (ExecClearTuple) to signal end of scan.
 */
TupleTableSlot *
levelPivotIterateForeignScan(ForeignScanState *node)
{
    TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
    Relation rel = node->ss.ss_currentRelation;
    ForeignTable *table = GetForeignTable(RelationGetRelid(rel));
    TableMode mode = get_table_mode(table);

    ExecClearTuple(slot);  /* Signal no more rows if we return early */

    if (mode == TableMode::RAW) {
        /* Raw mode iteration */
        auto state = static_cast<RawScanState *>(node->fdw_state);

        PG_TRY_CPP_RETURN({
            auto row = state->scanner->next_row();
            if (!row)
                return slot;

            /* Switch to temp context for value conversion */
            MemoryContext oldctx = MemoryContextSwitchTo(state->temp_context);

            /* Build tuple - raw tables have 2 columns: key, value */
            TupleDesc tupdesc = slot->tts_tupleDescriptor;
            Datum *values = slot->tts_values;
            bool *nulls = slot->tts_isnull;

            /* Initialize all to NULL */
            memset(nulls, true, tupdesc->natts * sizeof(bool));

            /* Find key and value columns and set their values */
            for (int i = 0; i < tupdesc->natts; i++) {
                Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
                if (attr->attisdropped)
                    continue;

                const char *colname = NameStr(attr->attname);
                if (strcmp(colname, "key") == 0) {
                    values[i] = CStringGetTextDatum(row->key.c_str());
                    nulls[i] = false;
                } else if (strcmp(colname, "value") == 0) {
                    values[i] = CStringGetTextDatum(row->value.c_str());
                    nulls[i] = false;
                }
            }

            MemoryContextSwitchTo(oldctx);
            MemoryContextReset(state->temp_context);

            ExecStoreVirtualTuple(slot);
            return slot;
        }, slot);
    } else {
        /* Pivot mode iteration */
        auto state = static_cast<LevelPivotScanState *>(node->fdw_state);

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
}

/*
 * ReScanForeignScan
 *      Restart the scan from the beginning
 */
void
levelPivotReScanForeignScan(ForeignScanState *node)
{
    Relation rel = node->ss.ss_currentRelation;
    ForeignTable *table = GetForeignTable(RelationGetRelid(rel));
    TableMode mode = get_table_mode(table);

    if (mode == TableMode::RAW) {
        auto state = static_cast<RawScanState *>(node->fdw_state);
        PG_TRY_CPP({
            state->scanner->rescan();
        });
    } else {
        auto state = static_cast<LevelPivotScanState *>(node->fdw_state);
        PG_TRY_CPP({
            state->scanner->begin_scan(state->prefix_values);
        });
    }
}

/*
 * EndForeignScan
 *      Clean up the scan state
 */
void
levelPivotEndForeignScan(ForeignScanState *node)
{
    if (!node->fdw_state)
        return;

    Relation rel = node->ss.ss_currentRelation;
    ForeignTable *table = GetForeignTable(RelationGetRelid(rel));
    TableMode mode = get_table_mode(table);

    if (mode == TableMode::RAW) {
        auto state = static_cast<RawScanState *>(node->fdw_state);
        state->cleanup();
    } else {
        auto state = static_cast<LevelPivotScanState *>(node->fdw_state);
        state->cleanup();
    }
    node->fdw_state = nullptr;
}

/*
 * ExplainForeignScan
 *      Print additional EXPLAIN output
 */
void
levelPivotExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
    Relation rel = node->ss.ss_currentRelation;
    ForeignTable *table = GetForeignTable(RelationGetRelid(rel));
    TableMode mode = get_table_mode(table);
    ForeignScan *fsplan = (ForeignScan *) node->ss.ps.plan;

    if (mode == TableMode::RAW) {
        /* Raw mode: show key bounds */
        auto state = static_cast<RawScanState *>(node->fdw_state);

        if (fsplan->fdw_private != NIL) {
            List *fdw_private = fsplan->fdw_private;
            ListCell *cell = list_head(fdw_private);

            /* Check for raw mode marker */
            if (cell != NULL && intVal(lfirst(cell)) == -1) {
                cell = lnext(fdw_private, cell);

                std::string bounds_desc;
                while (cell != NULL) {
                    int strategy = intVal(lfirst(cell));
                    cell = lnext(fdw_private, cell);
                    if (cell == NULL)
                        break;
                    char *value = strVal(lfirst(cell));
                    cell = lnext(fdw_private, cell);

                    if (!bounds_desc.empty())
                        bounds_desc += ", ";

                    switch (strategy) {
                        case BTEqualStrategyNumber:
                            bounds_desc += "key='";
                            bounds_desc += value;
                            bounds_desc += "'";
                            break;
                        case BTLessStrategyNumber:
                            bounds_desc += "key<'";
                            bounds_desc += value;
                            bounds_desc += "'";
                            break;
                        case BTLessEqualStrategyNumber:
                            bounds_desc += "key<='";
                            bounds_desc += value;
                            bounds_desc += "'";
                            break;
                        case BTGreaterStrategyNumber:
                            bounds_desc += "key>'";
                            bounds_desc += value;
                            bounds_desc += "'";
                            break;
                        case BTGreaterEqualStrategyNumber:
                            bounds_desc += "key>='";
                            bounds_desc += value;
                            bounds_desc += "'";
                            break;
                    }
                }

                if (!bounds_desc.empty()) {
                    ExplainPropertyText("LevelDB Key Bounds", bounds_desc.c_str(), es);
                }
            }
        }

        if (state && state->scanner) {
            const auto& stats = state->scanner->stats();
            ExplainPropertyInteger("LevelDB Keys Scanned", NULL,
                                  stats.keys_scanned, es);
        }
    } else {
        /* Pivot mode */
        auto state = static_cast<LevelPivotScanState *>(node->fdw_state);

        if (fsplan->fdw_private != NIL) {
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
}

/*
 * AddForeignUpdateTargets - Tell PostgreSQL what we need to identify rows.
 *
 * For UPDATE/DELETE, PostgreSQL needs to pass us the "old" row values
 * so we can find the right keys to modify. We request the whole row
 * as a junk attribute (not returned to user, just used internally).
 *
 * Alternative: Could use just identity columns, but whole row is simpler
 * and handles edge cases like identity column changes in UPDATE.
 */
void
levelPivotAddForeignUpdateTargets(PlannerInfo *root,
                                  Index rtindex,
                                  RangeTblEntry *target_rte,
                                  Relation target_relation)
{
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
 * BeginForeignModify - Initialize state for INSERT/UPDATE/DELETE.
 *
 * Creates Writer (or RawWriter) with optional WriteBatch support.
 * WriteBatch provides atomicity: all row modifications in a statement
 * either succeed together or fail together.
 *
 * Also captures schema/table names for NOTIFY support.
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
        TableMode mode = get_table_mode(table);

        /* Get options */
        auto conn_options = get_server_options(server);
        conn_options.read_only = false;  /* Need write access */

        /* Create dedicated memory context for modify state */
        MemoryContext modify_ctx = AllocSetContextCreate(estate->es_query_cxt,
                                                         "level_pivot modify",
                                                         ALLOCSET_DEFAULT_SIZES);

        if (mode == TableMode::RAW) {
            /* Raw mode: use RawWriter */
            auto state = level_pivot::pg_construct<RawModifyState>(modify_ctx);

            /* Get connection */
            state->connection = level_pivot::ConnectionManager::instance()
                .get_connection(server->serverid, conn_options);

            /* Store write batch setting */
            state->use_write_batch = conn_options.use_write_batch;

            /* Create writer */
            state->writer = std::make_unique<level_pivot::RawWriter>(
                state->connection, conn_options.use_write_batch);

            /* Find key and value columns */
            state->key_attnum = find_column_attnum(rel, "key");
            state->value_attnum = find_column_attnum(rel, "value");

            /* Capture table metadata for NOTIFY */
            state->schema_name = get_namespace_name(RelationGetNamespace(rel));
            state->table_name = RelationGetRelationName(rel);

            rinfo->ri_FdwState = state;
        } else {
            /* Pivot mode: use Writer */
            std::string key_pattern = get_table_option(table, "key_pattern");

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

            /* Capture table metadata for NOTIFY */
            state->schema_name = get_namespace_name(RelationGetNamespace(rel));
            state->table_name = RelationGetRelationName(rel);

            rinfo->ri_FdwState = state;
        }
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
    Relation rel = rinfo->ri_RelationDesc;
    ForeignTable *table = GetForeignTable(RelationGetRelid(rel));
    TableMode mode = get_table_mode(table);

    if (mode == TableMode::RAW) {
        auto state = static_cast<RawModifyState *>(rinfo->ri_FdwState);

        PG_TRY_CPP_RETURN({
            slot_getallattrs(slot);

            /* Extract key and value from slot */
            int key_idx = state->key_attnum - 1;
            int val_idx = state->value_attnum - 1;

            if (slot->tts_isnull[key_idx])
                elog(ERROR, "key column cannot be NULL");

            std::string key = TextDatumGetCString(slot->tts_values[key_idx]);
            std::string value;
            if (!slot->tts_isnull[val_idx])
                value = TextDatumGetCString(slot->tts_values[val_idx]);

            state->writer->insert(key, value);
            state->has_modifications = true;
            return slot;
        }, slot);
    } else {
        auto state = static_cast<LevelPivotModifyState *>(rinfo->ri_FdwState);

        PG_TRY_CPP_RETURN({
            slot_getallattrs(slot);
            state->writer->insert(slot->tts_values, slot->tts_isnull);
            state->has_modifications = true;
            return slot;
        }, slot);
    }
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
    Relation rel = rinfo->ri_RelationDesc;
    ForeignTable *table = GetForeignTable(RelationGetRelid(rel));
    TableMode mode = get_table_mode(table);

    if (mode == TableMode::RAW) {
        auto state = static_cast<RawModifyState *>(rinfo->ri_FdwState);

        PG_TRY_CPP_RETURN({
            /* Get the old row from the wholerow junk attribute */
            bool isnull;
            Datum datum = ExecGetJunkAttribute(planSlot,
                              rinfo->ri_RowIdAttNo,
                              &isnull);

            if (isnull)
                elog(ERROR, "wholerow is NULL");

            HeapTupleHeader oldtup = DatumGetHeapTupleHeader(datum);

            /* Extract key from old row */
            int key_idx = state->key_attnum;
            bool key_null;
            Datum key_datum = GetAttributeByNum(oldtup, key_idx, &key_null);

            if (key_null)
                elog(ERROR, "key column cannot be NULL");

            std::string key = TextDatumGetCString(key_datum);

            /* Get new value from slot */
            slot_getallattrs(slot);
            int val_idx = state->value_attnum - 1;

            std::string new_value;
            if (!slot->tts_isnull[val_idx])
                new_value = TextDatumGetCString(slot->tts_values[val_idx]);

            state->writer->update(key, new_value);
            state->has_modifications = true;
            return slot;
        }, slot);
    } else {
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

            state->has_modifications = true;
            return slot;
        }, slot);
    }
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
    Relation rel = rinfo->ri_RelationDesc;
    ForeignTable *table = GetForeignTable(RelationGetRelid(rel));
    TableMode mode = get_table_mode(table);

    if (mode == TableMode::RAW) {
        auto state = static_cast<RawModifyState *>(rinfo->ri_FdwState);

        PG_TRY_CPP_RETURN({
            /* Get the old row from the wholerow junk attribute */
            bool isnull;
            Datum datum = ExecGetJunkAttribute(planSlot,
                              rinfo->ri_RowIdAttNo,
                              &isnull);

            if (isnull)
                elog(ERROR, "wholerow is NULL");

            HeapTupleHeader oldtup = DatumGetHeapTupleHeader(datum);

            /* Extract key from old row */
            int key_idx = state->key_attnum;
            bool key_null;
            Datum key_datum = GetAttributeByNum(oldtup, key_idx, &key_null);

            if (key_null)
                elog(ERROR, "key column cannot be NULL");

            std::string key = TextDatumGetCString(key_datum);
            state->writer->remove(key);
            state->has_modifications = true;

            return slot;
        }, slot);
    } else {
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
            state->has_modifications = true;

            return slot;
        }, slot);
    }
}

/*
 * EndForeignModify - Commit changes and clean up.
 *
 * This is called after the last row modification. Key responsibilities:
 *   1. Commit WriteBatch (applies all accumulated operations atomically)
 *   2. Send NOTIFY if any modifications occurred
 *   3. Release writer and connection resources
 *
 * NOTIFY is sent after commit so listeners see consistent data.
 */
void
levelPivotEndForeignModify(EState *estate, ResultRelInfo *rinfo)
{
    if (!rinfo->ri_FdwState)
        return;

    Relation rel = rinfo->ri_RelationDesc;
    ForeignTable *table = GetForeignTable(RelationGetRelid(rel));
    TableMode mode = get_table_mode(table);

    if (mode == TableMode::RAW) {
        auto state = static_cast<RawModifyState *>(rinfo->ri_FdwState);

        PG_TRY_CPP({
            /* Commit makes all accumulated operations visible atomically */
            if (state->writer && state->use_write_batch) {
                state->writer->commit_batch();
            }

            /* NOTIFY lets LISTEN clients react to changes */
            if (state->has_modifications) {
                send_table_changed_notify(state->schema_name, state->table_name);
            }
        });

        state->cleanup();
    } else {
        auto state = static_cast<LevelPivotModifyState *>(rinfo->ri_FdwState);

        PG_TRY_CPP({
            /* Commit batch if using batched writes */
            if (state->writer && state->use_write_batch) {
                state->writer->commit_batch();
            }

            /* Send NOTIFY if modifications occurred */
            if (state->has_modifications) {
                send_table_changed_notify(state->schema_name, state->table_name);
            }
        });

        state->cleanup();
    }
    rinfo->ri_FdwState = nullptr;
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
 * ImportForeignSchema - Auto-generate table definitions from LevelDB data.
 *
 * Called by: IMPORT FOREIGN SCHEMA remote_schema FROM SERVER srv INTO local_schema;
 *
 * Algorithm:
 *   1. Sample keys to infer delimiter and structure
 *   2. Identify constant vs variable segments
 *   3. Generate pattern with {colN} placeholders
 *   4. Scan for all attr names (the {attr} values)
 *   5. Generate CREATE FOREIGN TABLE with discovered columns
 *
 * This is a best-effort heuristic. Complex key structures may need
 * manual table definitions.
 */
List *
levelPivotImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
    List *commands = NIL;

    PG_TRY_CPP_RETURN({
        ForeignServer *server = GetForeignServer(serverOid);
        auto conn_options = get_server_options(server);

        auto connection = level_pivot::ConnectionManager::instance()
            .get_connection(server->serverid, conn_options);

        level_pivot::SchemaDiscovery discovery(connection);

        /* Infer pattern by analyzing key structure across samples */
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
