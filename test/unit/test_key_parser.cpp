#include <gtest/gtest.h>
#include "level_pivot/key_parser.hpp"

using namespace level_pivot;

class KeyParserTest : public ::testing::Test {};

TEST_F(KeyParserTest, ParseSimpleKey) {
    KeyParser parser("users##{group}##{id}##{attr}");

    auto result = parser.parse("users##admins##user001##name");
    ASSERT_TRUE(result.has_value());

    ASSERT_EQ(result->capture_values.size(), 2);
    EXPECT_EQ(result->capture_values[0], "admins");
    EXPECT_EQ(result->capture_values[1], "user001");
    EXPECT_EQ(result->attr_name, "name");
}

TEST_F(KeyParserTest, ParseMixedDelimiters) {
    KeyParser parser("this###{arg}__{sub_arg}##pat##{attr}");

    auto result = parser.parse("this###sales__west##pat##revenue");
    ASSERT_TRUE(result.has_value());

    ASSERT_EQ(result->capture_values.size(), 2);
    EXPECT_EQ(result->capture_values[0], "sales");
    EXPECT_EQ(result->capture_values[1], "west");
    EXPECT_EQ(result->attr_name, "revenue");
}

TEST_F(KeyParserTest, ParseDifferentDelimiters) {
    KeyParser parser("{tenant}:{env}/{service}/{attr}");

    auto result = parser.parse("acme:prod/api/requests");
    ASSERT_TRUE(result.has_value());

    ASSERT_EQ(result->capture_values.size(), 3);
    EXPECT_EQ(result->capture_values[0], "acme");
    EXPECT_EQ(result->capture_values[1], "prod");
    EXPECT_EQ(result->capture_values[2], "api");
    EXPECT_EQ(result->attr_name, "requests");
}

TEST_F(KeyParserTest, ParseNoMatch_WrongPrefix) {
    KeyParser parser("users##{group}##{id}##{attr}");

    auto result = parser.parse("groups##admins##user001##name");
    EXPECT_FALSE(result.has_value());
}

TEST_F(KeyParserTest, ParseNoMatch_MissingDelimiter) {
    KeyParser parser("users##{group}##{id}##{attr}");

    auto result = parser.parse("users##adminsuser001##name");
    EXPECT_FALSE(result.has_value());
}

TEST_F(KeyParserTest, ParseNoMatch_EmptyCapture) {
    KeyParser parser("users##{group}##{id}##{attr}");

    auto result = parser.parse("users####user001##name");
    EXPECT_FALSE(result.has_value());
}

TEST_F(KeyParserTest, ParseWithTrailingContent) {
    // When {attr} is at the end of a pattern, it captures everything remaining
    KeyParser parser("users##{group}##{id}##{attr}");

    auto result = parser.parse("users##admins##user001##name##extra");
    ASSERT_TRUE(result.has_value());
    // The attr captures everything after the last delimiter
    EXPECT_EQ(result->attr_name, "name##extra");
}

TEST_F(KeyParserTest, Matches) {
    KeyParser parser("users##{group}##{id}##{attr}");

    EXPECT_TRUE(parser.matches("users##admins##user001##name"));
    EXPECT_FALSE(parser.matches("groups##admins##user001##name"));
    EXPECT_FALSE(parser.matches("users##admins##name"));
}

TEST_F(KeyParserTest, BuildKey) {
    KeyParser parser("users##{group}##{id}##{attr}");

    std::vector<std::string> captures = {"admins", "user001"};
    std::string key = parser.build(captures, "email");
    EXPECT_EQ(key, "users##admins##user001##email");
}

TEST_F(KeyParserTest, BuildKeyMixedDelimiters) {
    KeyParser parser("this###{arg}__{sub_arg}##pat##{attr}");

    std::vector<std::string> captures = {"sales", "west"};
    std::string key = parser.build(captures, "revenue");
    EXPECT_EQ(key, "this###sales__west##pat##revenue");
}

TEST_F(KeyParserTest, BuildKeyWithMap) {
    KeyParser parser("users##{group}##{id}##{attr}");

    std::unordered_map<std::string, std::string> captures = {
        {"group", "admins"},
        {"id", "user001"}
    };

    std::string key = parser.build(captures, "name");
    EXPECT_EQ(key, "users##admins##user001##name");
}

TEST_F(KeyParserTest, BuildKeyErrorWrongCount) {
    KeyParser parser("users##{group}##{id}##{attr}");

    std::vector<std::string> one = {"admins"};
    std::vector<std::string> three = {"a", "b", "c"};
    EXPECT_THROW(parser.build(one, "name"), std::invalid_argument);
    EXPECT_THROW(parser.build(three, "name"), std::invalid_argument);
}

TEST_F(KeyParserTest, BuildKeyErrorEmptyCapture) {
    KeyParser parser("users##{group}##{id}##{attr}");

    std::vector<std::string> captures = {"", "user001"};
    EXPECT_THROW(parser.build(captures, "name"), std::invalid_argument);
}

TEST_F(KeyParserTest, BuildKeyErrorEmptyAttr) {
    KeyParser parser("users##{group}##{id}##{attr}");

    std::vector<std::string> captures = {"admins", "user001"};
    EXPECT_THROW(parser.build(captures, ""), std::invalid_argument);
}

TEST_F(KeyParserTest, BuildKeyErrorMissingCapture) {
    KeyParser parser("users##{group}##{id}##{attr}");

    std::unordered_map<std::string, std::string> captures = {
        {"group", "admins"}
        // missing "id"
    };

    EXPECT_THROW(parser.build(captures, "name"), std::invalid_argument);
}

TEST_F(KeyParserTest, BuildPrefix) {
    KeyParser parser("users##{group}##{id}##{attr}");

    EXPECT_EQ(parser.build_prefix(), "users##");
}

TEST_F(KeyParserTest, BuildPrefixWithCaptures) {
    KeyParser parser("users##{group}##{id}##{attr}");

    std::vector<std::string> empty = {};
    std::vector<std::string> one = {"admins"};
    std::vector<std::string> two = {"admins", "user001"};
    EXPECT_EQ(parser.build_prefix(empty), "users##");
    EXPECT_EQ(parser.build_prefix(one), "users##admins##");
    EXPECT_EQ(parser.build_prefix(two), "users##admins##user001##");
}

TEST_F(KeyParserTest, BuildPrefixNoLiteral) {
    KeyParser parser("{tenant}:{env}/{attr}");

    std::vector<std::string> one = {"acme"};
    EXPECT_EQ(parser.build_prefix(), "");
    EXPECT_EQ(parser.build_prefix(one), "acme:");
}

TEST_F(KeyParserTest, StartsWithPrefix) {
    KeyParser parser("users##{group}##{id}##{attr}");

    EXPECT_TRUE(parser.starts_with_prefix("users##admins##user001##name"));
    EXPECT_TRUE(parser.starts_with_prefix("users##anything"));
    EXPECT_FALSE(parser.starts_with_prefix("user##admins##user001##name"));
    EXPECT_FALSE(parser.starts_with_prefix(""));
}

TEST_F(KeyParserTest, RoundTrip) {
    KeyParser parser("users##{group}##{id}##{attr}");

    std::string original = "users##admins##user001##email";

    auto parsed = parser.parse(original);
    ASSERT_TRUE(parsed.has_value());

    std::string rebuilt = parser.build(parsed->capture_values, parsed->attr_name);
    EXPECT_EQ(original, rebuilt);
}

TEST_F(KeyParserTest, RoundTripMixedDelimiters) {
    KeyParser parser("this###{arg}__{sub_arg}##pat##{attr}");

    std::string original = "this###marketing__east##pat##count";

    auto parsed = parser.parse(original);
    ASSERT_TRUE(parsed.has_value());

    std::string rebuilt = parser.build(parsed->capture_values, parsed->attr_name);
    EXPECT_EQ(original, rebuilt);
}

TEST_F(KeyParserTest, CaptureWithSpecialChars) {
    // Capture values can contain special characters
    KeyParser parser("users##{group}##{id}##{attr}");

    auto result = parser.parse("users##admin/special##user:001##name");
    ASSERT_TRUE(result.has_value());

    EXPECT_EQ(result->capture_values[0], "admin/special");
    EXPECT_EQ(result->capture_values[1], "user:001");
}
