/**
 * FDW option validation
 */

// PostgreSQL headers must come first for Windows compatibility
extern "C" {
#include "postgres.h"
#include "fmgr.h"
#include "foreign/foreign.h"
#include "commands/defrem.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
}

#include "level_pivot/key_pattern.hpp"
#include <string>
#include <unordered_set>
#include <cstring>

namespace {

/* Valid SERVER options */
const std::unordered_set<std::string> server_options = {
    "db_path",
    "read_only",
    "create_if_missing",
    "block_cache_size",
    "write_buffer_size"
};

/* Valid FOREIGN TABLE options */
const std::unordered_set<std::string> table_options = {
    "key_pattern",
    "prefix_filter"
};

bool is_valid_bool(const char* value) {
    std::string v(value);
    return v == "true" || v == "false" || v == "on" || v == "off" ||
           v == "1" || v == "0" || v == "yes" || v == "no";
}

bool is_valid_size(const char* value) {
    char* end;
    long long num = strtoll(value, &end, 10);
    if (*end != '\0' && *end != 'k' && *end != 'K' &&
        *end != 'm' && *end != 'M' && *end != 'g' && *end != 'G') {
        return false;
    }
    return num >= 0;
}

} // anonymous namespace

extern "C" {

void levelPivotValidateOptions(List *options_list, Oid catalog)
{
    ListCell *cell;

    foreach(cell, options_list)
    {
        DefElem *def = (DefElem *) lfirst(cell);
        std::string name(def->defname);

        if (catalog == ForeignServerRelationId)
        {
            /* Validate SERVER options */
            if (server_options.find(name) == server_options.end())
            {
                ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                     errmsg("invalid option \"%s\" for SERVER", def->defname),
                     errhint("Valid options are: db_path, read_only, "
                            "create_if_missing, block_cache_size, write_buffer_size")));
            }

            const char* value = defGetString(def);

            if (name == "db_path")
            {
                if (strlen(value) == 0)
                {
                    ereport(ERROR,
                        (errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
                         errmsg("db_path cannot be empty")));
                }
            }
            else if (name == "read_only" || name == "create_if_missing")
            {
                if (!is_valid_bool(value))
                {
                    ereport(ERROR,
                        (errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
                         errmsg("invalid value for %s: \"%s\"",
                               def->defname, value),
                         errhint("Use 'true' or 'false'")));
                }
            }
            else if (name == "block_cache_size" || name == "write_buffer_size")
            {
                if (!is_valid_size(value))
                {
                    ereport(ERROR,
                        (errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
                         errmsg("invalid value for %s: \"%s\"",
                               def->defname, value),
                         errhint("Use a positive integer, optionally with K/M/G suffix")));
                }
            }
        }
        else if (catalog == ForeignTableRelationId)
        {
            /* Validate FOREIGN TABLE options */
            if (table_options.find(name) == table_options.end())
            {
                ereport(ERROR,
                    (errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
                     errmsg("invalid option \"%s\" for FOREIGN TABLE", def->defname),
                     errhint("Valid options are: key_pattern, prefix_filter")));
            }

            const char* value = defGetString(def);

            if (name == "key_pattern")
            {
                /* Validate the key pattern syntax */
                try {
                    level_pivot::KeyPattern pattern(value);

                    if (!pattern.has_attr())
                    {
                        ereport(ERROR,
                            (errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
                             errmsg("key_pattern must contain {attr} segment"),
                             errhint("Example: 'users##{group}##{id}##{attr}'")));
                    }
                } catch (const level_pivot::KeyPatternError& e) {
                    ereport(ERROR,
                        (errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
                         errmsg("invalid key_pattern: %s", e.what())));
                }
            }
        }
    }

    /* Check required options */
    if (catalog == ForeignServerRelationId)
    {
        bool has_db_path = false;
        foreach(cell, options_list)
        {
            DefElem *def = (DefElem *) lfirst(cell);
            if (strcmp(def->defname, "db_path") == 0)
            {
                has_db_path = true;
                break;
            }
        }

        if (!has_db_path)
        {
            ereport(ERROR,
                (errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
                 errmsg("required option \"db_path\" not specified")));
        }
    }
    else if (catalog == ForeignTableRelationId)
    {
        bool has_key_pattern = false;
        foreach(cell, options_list)
        {
            DefElem *def = (DefElem *) lfirst(cell);
            if (strcmp(def->defname, "key_pattern") == 0)
            {
                has_key_pattern = true;
                break;
            }
        }

        if (!has_key_pattern)
        {
            ereport(ERROR,
                (errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
                 errmsg("required option \"key_pattern\" not specified")));
        }
    }
}

} /* extern "C" */
