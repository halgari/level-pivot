#include <gtest/gtest.h>
#include "level_pivot/schema_discovery.hpp"
#include "level_pivot/connection_manager.hpp"
#include <filesystem>

using namespace level_pivot;

class SchemaDiscoveryTest : public ::testing::Test {
protected:
    std::string test_db_path_;
    std::shared_ptr<LevelDBConnection> connection_;

    void SetUp() override {
        test_db_path_ = "/tmp/level_pivot_schema_discovery_test_" +
                        std::to_string(getpid());

        // Clean up any previous test database
        std::filesystem::remove_all(test_db_path_);

        ConnectionOptions opts;
        opts.db_path = test_db_path_;
        opts.read_only = false;
        opts.create_if_missing = true;

        connection_ = std::make_shared<LevelDBConnection>(opts);
    }

    void TearDown() override {
        connection_.reset();
        std::filesystem::remove_all(test_db_path_);
    }

    void populateUsersData() {
        // Populate with users## pattern data
        connection_->put("users##admins##user001##name", "Alice");
        connection_->put("users##admins##user001##email", "alice@example.com");
        connection_->put("users##admins##user001##role", "admin");
        connection_->put("users##admins##user002##name", "Bob");
        connection_->put("users##admins##user002##email", "bob@example.com");
        connection_->put("users##guests##user003##name", "Charlie");
        connection_->put("users##guests##user003##email", "charlie@example.com");
    }

    void populateMixedData() {
        // Different patterns
        connection_->put("metrics:prod/web/requests", "1000");
        connection_->put("metrics:prod/web/latency", "50");
        connection_->put("metrics:prod/api/requests", "500");
        connection_->put("metrics:prod/api/latency", "100");
        connection_->put("metrics:staging/web/requests", "10");
        connection_->put("other_prefix##foo##bar", "value");
    }
};

// Test discover() with a known pattern
TEST_F(SchemaDiscoveryTest, DiscoverAttrsWithPattern) {
    populateUsersData();

    SchemaDiscovery discovery(connection_);
    KeyPattern pattern("users##{group}##{id}##{attr}");

    auto result = discovery.discover(pattern);

    EXPECT_EQ(result.keys_scanned, 7);
    EXPECT_EQ(result.keys_matched, 7);
    EXPECT_EQ(result.attrs.size(), 3);  // name, email, role

    // Attrs should be sorted by frequency (descending)
    // name and email appear 3 times, role appears 1 time
    std::unordered_set<std::string> attr_names;
    for (const auto& attr : result.attrs) {
        attr_names.insert(attr.name);
    }

    EXPECT_TRUE(attr_names.count("name") > 0);
    EXPECT_TRUE(attr_names.count("email") > 0);
    EXPECT_TRUE(attr_names.count("role") > 0);
}

// Test discover() with prefix filter
TEST_F(SchemaDiscoveryTest, DiscoverWithPrefixFilter) {
    populateUsersData();

    SchemaDiscovery discovery(connection_);
    KeyPattern pattern("users##{group}##{id}##{attr}");

    DiscoveryOptions opts;
    opts.prefix_filter = "users##admins##";

    auto result = discovery.discover(pattern, opts);

    // Should only scan admins group (5 keys)
    EXPECT_EQ(result.keys_matched, 5);

    std::unordered_set<std::string> attr_names;
    for (const auto& attr : result.attrs) {
        attr_names.insert(attr.name);
    }

    EXPECT_EQ(attr_names.size(), 3);  // name, email, role
}

// Test discover() with max_keys limit
TEST_F(SchemaDiscoveryTest, DiscoverWithMaxKeysLimit) {
    populateUsersData();

    SchemaDiscovery discovery(connection_);
    KeyPattern pattern("users##{group}##{id}##{attr}");

    DiscoveryOptions opts;
    opts.max_keys = 3;

    auto result = discovery.discover(pattern, opts);

    EXPECT_EQ(result.keys_scanned, 3);
}

// Test discover() with pattern that doesn't match
TEST_F(SchemaDiscoveryTest, DiscoverNoMatches) {
    populateUsersData();

    SchemaDiscovery discovery(connection_);
    KeyPattern pattern("nonexistent##{id}##{attr}");

    auto result = discovery.discover(pattern);

    EXPECT_EQ(result.keys_scanned, 0);
    EXPECT_EQ(result.keys_matched, 0);
    EXPECT_TRUE(result.attrs.empty());
}

// Test list_prefixes()
TEST_F(SchemaDiscoveryTest, ListPrefixes) {
    populateUsersData();
    populateMixedData();

    SchemaDiscovery discovery(connection_);

    auto prefixes = discovery.list_prefixes(1, 10);

    EXPECT_FALSE(prefixes.empty());

    // Should find different prefix groups
    bool found_metrics = false;
    bool found_users = false;
    for (const auto& p : prefixes) {
        if (p.find("metrics") != std::string::npos) found_metrics = true;
        if (p.find("users") != std::string::npos) found_users = true;
    }

    EXPECT_TRUE(found_metrics);
    EXPECT_TRUE(found_users);
}

// Test infer_pattern() with consistent delimiter
TEST_F(SchemaDiscoveryTest, InferPatternWithDoubleHash) {
    populateUsersData();

    SchemaDiscovery discovery(connection_);

    auto pattern = discovery.infer_pattern(100);

    ASSERT_TRUE(pattern.has_value());

    // Should contain ## delimiter and {attr} placeholder
    EXPECT_TRUE(pattern->find("##") != std::string::npos);
    EXPECT_TRUE(pattern->find("{attr}") != std::string::npos);
}

// Test infer_pattern() with colon/slash delimiter
TEST_F(SchemaDiscoveryTest, InferPatternWithMixedDelimiters) {
    // Clear and add only metrics data
    connection_.reset();
    std::filesystem::remove_all(test_db_path_);

    ConnectionOptions opts;
    opts.db_path = test_db_path_;
    opts.read_only = false;
    opts.create_if_missing = true;
    connection_ = std::make_shared<LevelDBConnection>(opts);

    connection_->put("metrics:prod/web/requests", "1000");
    connection_->put("metrics:prod/web/latency", "50");
    connection_->put("metrics:prod/api/requests", "500");
    connection_->put("metrics:staging/web/requests", "10");

    SchemaDiscovery discovery(connection_);

    auto pattern = discovery.infer_pattern(100);

    ASSERT_TRUE(pattern.has_value());

    // Should detect one of the delimiters and have {attr}
    EXPECT_TRUE(pattern->find("{attr}") != std::string::npos);
}

// Test infer_pattern() with empty database
TEST_F(SchemaDiscoveryTest, InferPatternEmptyDatabase) {
    SchemaDiscovery discovery(connection_);

    auto pattern = discovery.infer_pattern(100);

    EXPECT_FALSE(pattern.has_value());
}

// Test generate_foreign_table_sql()
TEST_F(SchemaDiscoveryTest, GenerateForeignTableSql) {
    populateUsersData();

    SchemaDiscovery discovery(connection_);
    KeyPattern pattern("users##{group}##{id}##{attr}");

    auto result = discovery.discover(pattern);

    std::string sql = generate_foreign_table_sql(
        "discovered_users",
        "test_server",
        "users##{group}##{id}##{attr}",
        result);

    // Should contain table name
    EXPECT_TRUE(sql.find("CREATE FOREIGN TABLE discovered_users") != std::string::npos);

    // Should contain server name
    EXPECT_TRUE(sql.find("SERVER test_server") != std::string::npos);

    // Should contain identity columns
    EXPECT_TRUE(sql.find("group TEXT") != std::string::npos);
    EXPECT_TRUE(sql.find("id TEXT") != std::string::npos);

    // Should contain discovered attr columns
    EXPECT_TRUE(sql.find("name TEXT") != std::string::npos);
    EXPECT_TRUE(sql.find("email TEXT") != std::string::npos);

    // Should contain key_pattern option
    EXPECT_TRUE(sql.find("key_pattern 'users##{group}##{id}##{attr}'") != std::string::npos);
}

// Test sample value collection
TEST_F(SchemaDiscoveryTest, SampleValueCollection) {
    populateUsersData();

    SchemaDiscovery discovery(connection_);
    KeyPattern pattern("users##{group}##{id}##{attr}");

    auto result = discovery.discover(pattern);

    // At least one attr should have a sample value
    bool has_sample = false;
    for (const auto& attr : result.attrs) {
        if (!attr.sample_value.empty()) {
            has_sample = true;
            break;
        }
    }

    EXPECT_TRUE(has_sample);
}
