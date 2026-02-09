#include <gtest/gtest.h>
#include "level_pivot/raw_scanner.hpp"
#include "level_pivot/connection_manager.hpp"
#include <filesystem>

using namespace level_pivot;

class RawScanBoundsTest : public ::testing::Test {};

// RawScanBounds unit tests (no LevelDB needed)

TEST_F(RawScanBoundsTest, UnboundedSeekStart) {
    RawScanBounds bounds;
    EXPECT_EQ(bounds.seek_start(), "");
    EXPECT_TRUE(bounds.is_unbounded());
}

TEST_F(RawScanBoundsTest, ExactMatchSeekStart) {
    RawScanBounds bounds;
    bounds.exact_key = "user:123";
    EXPECT_EQ(bounds.seek_start(), "user:123");
    EXPECT_TRUE(bounds.is_exact_match());
    EXPECT_FALSE(bounds.is_unbounded());
}

TEST_F(RawScanBoundsTest, LowerBoundSeekStart) {
    RawScanBounds bounds;
    bounds.lower_bound = "user:100";
    bounds.lower_inclusive = true;
    EXPECT_EQ(bounds.seek_start(), "user:100");
}

TEST_F(RawScanBoundsTest, UnboundedIsWithinBounds) {
    RawScanBounds bounds;
    EXPECT_TRUE(bounds.is_within_bounds("anything"));
    EXPECT_TRUE(bounds.is_within_bounds(""));
    EXPECT_TRUE(bounds.is_within_bounds("zzzzz"));
}

TEST_F(RawScanBoundsTest, ExactMatchIsWithinBounds) {
    RawScanBounds bounds;
    bounds.exact_key = "user:123";

    EXPECT_TRUE(bounds.is_within_bounds("user:123"));
    EXPECT_FALSE(bounds.is_within_bounds("user:124"));
    EXPECT_FALSE(bounds.is_within_bounds("user:122"));
    EXPECT_FALSE(bounds.is_within_bounds("user:12"));
}

TEST_F(RawScanBoundsTest, LowerBoundInclusiveIsWithinBounds) {
    RawScanBounds bounds;
    bounds.lower_bound = "user:100";
    bounds.lower_inclusive = true;

    EXPECT_TRUE(bounds.is_within_bounds("user:100"));
    EXPECT_TRUE(bounds.is_within_bounds("user:101"));
    EXPECT_TRUE(bounds.is_within_bounds("user:999"));
    EXPECT_FALSE(bounds.is_within_bounds("user:099"));
    EXPECT_FALSE(bounds.is_within_bounds("user:0"));
}

TEST_F(RawScanBoundsTest, LowerBoundExclusiveIsWithinBounds) {
    RawScanBounds bounds;
    bounds.lower_bound = "user:100";
    bounds.lower_inclusive = false;

    EXPECT_FALSE(bounds.is_within_bounds("user:100"));
    EXPECT_TRUE(bounds.is_within_bounds("user:101"));
    EXPECT_TRUE(bounds.is_within_bounds("user:999"));
}

TEST_F(RawScanBoundsTest, UpperBoundInclusiveIsWithinBounds) {
    RawScanBounds bounds;
    bounds.upper_bound = "user:200";
    bounds.upper_inclusive = true;

    EXPECT_TRUE(bounds.is_within_bounds("user:100"));
    EXPECT_TRUE(bounds.is_within_bounds("user:200"));
    EXPECT_FALSE(bounds.is_within_bounds("user:201"));
    EXPECT_FALSE(bounds.is_within_bounds("user:999"));
}

TEST_F(RawScanBoundsTest, UpperBoundExclusiveIsWithinBounds) {
    RawScanBounds bounds;
    bounds.upper_bound = "user:200";
    bounds.upper_inclusive = false;

    EXPECT_TRUE(bounds.is_within_bounds("user:100"));
    EXPECT_TRUE(bounds.is_within_bounds("user:199"));
    EXPECT_FALSE(bounds.is_within_bounds("user:200"));
    EXPECT_FALSE(bounds.is_within_bounds("user:201"));
}

TEST_F(RawScanBoundsTest, RangeBoundsIsWithinBounds) {
    RawScanBounds bounds;
    bounds.lower_bound = "user:100";
    bounds.lower_inclusive = true;
    bounds.upper_bound = "user:200";
    bounds.upper_inclusive = false;

    EXPECT_FALSE(bounds.is_within_bounds("user:099"));
    EXPECT_TRUE(bounds.is_within_bounds("user:100"));
    EXPECT_TRUE(bounds.is_within_bounds("user:150"));
    EXPECT_TRUE(bounds.is_within_bounds("user:199"));
    EXPECT_FALSE(bounds.is_within_bounds("user:200"));
    EXPECT_FALSE(bounds.is_within_bounds("user:201"));
}

TEST_F(RawScanBoundsTest, IsPastUpperBoundNoUpper) {
    RawScanBounds bounds;
    EXPECT_FALSE(bounds.is_past_upper_bound("anything"));
    EXPECT_FALSE(bounds.is_past_upper_bound("zzzzz"));
}

TEST_F(RawScanBoundsTest, IsPastUpperBoundExclusive) {
    RawScanBounds bounds;
    bounds.upper_bound = "user:200";
    bounds.upper_inclusive = false;

    EXPECT_FALSE(bounds.is_past_upper_bound("user:100"));
    EXPECT_FALSE(bounds.is_past_upper_bound("user:199"));
    EXPECT_TRUE(bounds.is_past_upper_bound("user:200"));
    EXPECT_TRUE(bounds.is_past_upper_bound("user:201"));
}

TEST_F(RawScanBoundsTest, IsPastUpperBoundInclusive) {
    RawScanBounds bounds;
    bounds.upper_bound = "user:200";
    bounds.upper_inclusive = true;

    EXPECT_FALSE(bounds.is_past_upper_bound("user:100"));
    EXPECT_FALSE(bounds.is_past_upper_bound("user:200"));
    EXPECT_TRUE(bounds.is_past_upper_bound("user:201"));
}

// RawScanner integration tests (need LevelDB)

class RawScannerTest : public ::testing::Test {
protected:
    std::string test_db_path_;
    std::shared_ptr<LevelDBConnection> connection_;

    void SetUp() override {
        test_db_path_ = "/tmp/level_pivot_raw_scanner_test_" +
                        std::to_string(getpid());

        // Clean up any previous test database
        std::filesystem::remove_all(test_db_path_);

        ConnectionOptions opts;
        opts.db_path = test_db_path_;
        opts.read_only = false;
        opts.create_if_missing = true;

        connection_ = std::make_shared<LevelDBConnection>(opts);

        // Populate test data
        connection_->put("user:001", "Alice");
        connection_->put("user:002", "Bob");
        connection_->put("user:003", "Charlie");
        connection_->put("user:010", "David");
        connection_->put("user:020", "Eve");
        connection_->put("other:001", "Other1");
        connection_->put("zzz:end", "End");
    }

    void TearDown() override {
        connection_.reset();
        std::filesystem::remove_all(test_db_path_);
    }
};

TEST_F(RawScannerTest, UnboundedScan) {
    RawScanner scanner(connection_);
    RawScanBounds bounds;

    scanner.begin_scan(bounds);

    std::vector<std::string> keys;
    while (auto row = scanner.next_row()) {
        keys.push_back(row->key);
    }

    ASSERT_EQ(keys.size(), 7);
    EXPECT_EQ(keys[0], "other:001");  // Sorted order
    EXPECT_EQ(keys[1], "user:001");
    EXPECT_EQ(keys[6], "zzz:end");
}

TEST_F(RawScannerTest, ExactMatchFound) {
    RawScanner scanner(connection_);
    RawScanBounds bounds;
    bounds.exact_key = "user:002";

    scanner.begin_scan(bounds);

    auto row = scanner.next_row();
    ASSERT_TRUE(row.has_value());
    EXPECT_EQ(row->key, "user:002");
    EXPECT_EQ(row->value, "Bob");

    // Should return no more rows
    EXPECT_FALSE(scanner.next_row().has_value());
}

TEST_F(RawScannerTest, ExactMatchNotFound) {
    RawScanner scanner(connection_);
    RawScanBounds bounds;
    bounds.exact_key = "user:999";

    scanner.begin_scan(bounds);

    EXPECT_FALSE(scanner.next_row().has_value());
}

TEST_F(RawScannerTest, RangeScanInclusive) {
    RawScanner scanner(connection_);
    RawScanBounds bounds;
    bounds.lower_bound = "user:002";
    bounds.lower_inclusive = true;
    bounds.upper_bound = "user:010";
    bounds.upper_inclusive = true;

    scanner.begin_scan(bounds);

    std::vector<std::string> keys;
    while (auto row = scanner.next_row()) {
        keys.push_back(row->key);
    }

    ASSERT_EQ(keys.size(), 3);
    EXPECT_EQ(keys[0], "user:002");
    EXPECT_EQ(keys[1], "user:003");
    EXPECT_EQ(keys[2], "user:010");
}

TEST_F(RawScannerTest, RangeScanExclusive) {
    RawScanner scanner(connection_);
    RawScanBounds bounds;
    bounds.lower_bound = "user:002";
    bounds.lower_inclusive = false;
    bounds.upper_bound = "user:010";
    bounds.upper_inclusive = false;

    scanner.begin_scan(bounds);

    std::vector<std::string> keys;
    while (auto row = scanner.next_row()) {
        keys.push_back(row->key);
    }

    ASSERT_EQ(keys.size(), 1);
    EXPECT_EQ(keys[0], "user:003");
}

TEST_F(RawScannerTest, PrefixScan) {
    RawScanner scanner(connection_);
    RawScanBounds bounds;
    bounds.lower_bound = "user:";
    bounds.lower_inclusive = true;
    bounds.upper_bound = "user:\xFF";  // Common prefix scan pattern
    bounds.upper_inclusive = false;

    scanner.begin_scan(bounds);

    std::vector<std::string> keys;
    while (auto row = scanner.next_row()) {
        keys.push_back(row->key);
    }

    ASSERT_EQ(keys.size(), 5);
    for (const auto& key : keys) {
        EXPECT_TRUE(key.substr(0, 5) == "user:");
    }
}

TEST_F(RawScannerTest, Rescan) {
    RawScanner scanner(connection_);
    RawScanBounds bounds;
    bounds.exact_key = "user:001";

    scanner.begin_scan(bounds);

    auto row1 = scanner.next_row();
    ASSERT_TRUE(row1.has_value());
    EXPECT_EQ(row1->key, "user:001");

    EXPECT_FALSE(scanner.next_row().has_value());

    // Rescan should restart
    scanner.rescan();

    auto row2 = scanner.next_row();
    ASSERT_TRUE(row2.has_value());
    EXPECT_EQ(row2->key, "user:001");
}

TEST_F(RawScannerTest, StatsTracking) {
    RawScanner scanner(connection_);
    RawScanBounds bounds;
    bounds.lower_bound = "user:";
    bounds.lower_inclusive = true;
    bounds.upper_bound = "user:\xFF";
    bounds.upper_inclusive = false;

    scanner.begin_scan(bounds);

    while (scanner.next_row()) {
        // consume all rows
    }

    const auto& stats = scanner.stats();
    EXPECT_EQ(stats.keys_scanned, 5);
}
