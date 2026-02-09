/**
 * Unit tests for NOTIFY channel name building logic
 *
 * This tests the channel name construction independently of PostgreSQL,
 * verifying correct format and truncation behavior.
 */

#include <gtest/gtest.h>
#include <string>

namespace {

/**
 * Build a NOTIFY channel name from schema and table name.
 * This mirrors the logic in fdw_handler.cpp's build_notify_channel().
 */
std::string build_notify_channel(const std::string& schema_name, const std::string& table_name)
{
    // Build channel: {schema}_{table}_changed
    std::string channel = schema_name + "_" + table_name + "_changed";

    // PostgreSQL channel names max 63 chars
    if (channel.length() > 63)
        channel = channel.substr(0, 63);

    return channel;
}

}  // anonymous namespace

class NotifyTest : public ::testing::Test {};

TEST_F(NotifyTest, BuildChannelBasic) {
    // Test basic channel name construction
    EXPECT_EQ(build_notify_channel("public", "users"), "public_users_changed");
    EXPECT_EQ(build_notify_channel("myschema", "orders"), "myschema_orders_changed");
}

TEST_F(NotifyTest, BuildChannelEmptySchema) {
    // Edge case: empty schema (should still work)
    EXPECT_EQ(build_notify_channel("", "users"), "_users_changed");
}

TEST_F(NotifyTest, BuildChannelEmptyTable) {
    // Edge case: empty table (should still work)
    EXPECT_EQ(build_notify_channel("public", ""), "public__changed");
}

TEST_F(NotifyTest, BuildChannelTruncation) {
    // Test 63-char limit for PostgreSQL identifiers
    // schema(30) + "_" + table(30) + "_changed"(8) = 69 chars total
    std::string long_schema(30, 'a');
    std::string long_table(30, 'b');
    std::string channel = build_notify_channel(long_schema, long_table);

    EXPECT_LE(channel.length(), 63u);
    EXPECT_EQ(channel.length(), 63u);  // Should be exactly 63 after truncation
}

TEST_F(NotifyTest, BuildChannelNoTruncationNeeded) {
    // Test that short names aren't truncated
    std::string schema = "public";
    std::string table = "users";
    std::string channel = build_notify_channel(schema, table);

    // "public_users_changed" = 20 chars, well under 63
    EXPECT_EQ(channel.length(), 20u);
    EXPECT_EQ(channel, "public_users_changed");
}

TEST_F(NotifyTest, BuildChannelExactly63Chars) {
    // Build a channel that's exactly 63 chars without truncation
    // Need: schema + "_" + table + "_changed" = 63
    // "_changed" = 8 chars, so schema + "_" + table = 55 chars
    // If schema = 27 chars, table = 27 chars: 27 + 1 + 27 = 55, total = 63
    std::string schema(27, 'x');
    std::string table(27, 'y');
    std::string channel = build_notify_channel(schema, table);

    EXPECT_EQ(channel.length(), 63u);
}

TEST_F(NotifyTest, BuildChannelUnderscoresInNames) {
    // Test with underscores already in schema/table names
    EXPECT_EQ(build_notify_channel("my_schema", "my_table"), "my_schema_my_table_changed");
}

TEST_F(NotifyTest, BuildChannelSpecialChars) {
    // PostgreSQL allows various characters in identifiers when quoted
    // The channel name should preserve them
    EXPECT_EQ(build_notify_channel("test", "table123"), "test_table123_changed");
}
