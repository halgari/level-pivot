#include <gtest/gtest.h>
#include "level_pivot/key_pattern.hpp"

using namespace level_pivot;

class KeyPatternTest : public ::testing::Test {};

TEST_F(KeyPatternTest, ParseSimplePattern) {
    KeyPattern pattern("users##{group}##{id}##{attr}");

    EXPECT_EQ(pattern.pattern(), "users##{group}##{id}##{attr}");
    EXPECT_EQ(pattern.capture_count(), 2);
    EXPECT_TRUE(pattern.has_attr());

    const auto& names = pattern.capture_names();
    ASSERT_EQ(names.size(), 2);
    EXPECT_EQ(names[0], "group");
    EXPECT_EQ(names[1], "id");
}

TEST_F(KeyPatternTest, ParseMixedDelimiters) {
    KeyPattern pattern("this###{arg}__{sub_arg}##pat##{attr}");

    EXPECT_EQ(pattern.capture_count(), 2);
    EXPECT_TRUE(pattern.has_attr());

    const auto& names = pattern.capture_names();
    ASSERT_EQ(names.size(), 2);
    EXPECT_EQ(names[0], "arg");
    EXPECT_EQ(names[1], "sub_arg");
}

TEST_F(KeyPatternTest, ParseDifferentDelimiters) {
    KeyPattern pattern("{tenant}:{env}/{service}/{attr}");

    EXPECT_EQ(pattern.capture_count(), 3);
    EXPECT_TRUE(pattern.has_attr());

    const auto& names = pattern.capture_names();
    ASSERT_EQ(names.size(), 3);
    EXPECT_EQ(names[0], "tenant");
    EXPECT_EQ(names[1], "env");
    EXPECT_EQ(names[2], "service");
}

TEST_F(KeyPatternTest, LiteralPrefix) {
    KeyPattern pattern1("users##{group}##{id}##{attr}");
    EXPECT_EQ(pattern1.literal_prefix(), "users##");

    KeyPattern pattern2("{tenant}:{env}/{attr}");
    EXPECT_EQ(pattern2.literal_prefix(), "");

    KeyPattern pattern3("prefix/fixed/{id}/{attr}");
    EXPECT_EQ(pattern3.literal_prefix(), "prefix/fixed/");
}

TEST_F(KeyPatternTest, Segments) {
    KeyPattern pattern("users##{group}##{id}##{attr}");
    const auto& segments = pattern.segments();

    ASSERT_EQ(segments.size(), 6);

    // Check segment types
    EXPECT_TRUE(std::holds_alternative<LiteralSegment>(segments[0]));
    EXPECT_TRUE(std::holds_alternative<CaptureSegment>(segments[1]));
    EXPECT_TRUE(std::holds_alternative<LiteralSegment>(segments[2]));
    EXPECT_TRUE(std::holds_alternative<CaptureSegment>(segments[3]));
    EXPECT_TRUE(std::holds_alternative<LiteralSegment>(segments[4]));
    EXPECT_TRUE(std::holds_alternative<AttrSegment>(segments[5]));

    // Check literal values
    EXPECT_EQ(std::get<LiteralSegment>(segments[0]).text, "users##");
    EXPECT_EQ(std::get<LiteralSegment>(segments[2]).text, "##");
    EXPECT_EQ(std::get<LiteralSegment>(segments[4]).text, "##");

    // Check capture names
    EXPECT_EQ(std::get<CaptureSegment>(segments[1]).name, "group");
    EXPECT_EQ(std::get<CaptureSegment>(segments[3]).name, "id");
}

TEST_F(KeyPatternTest, AttrIndex) {
    KeyPattern pattern("users##{group}##{id}##{attr}");
    EXPECT_EQ(pattern.attr_index(), 5);
}

TEST_F(KeyPatternTest, HasCapture) {
    KeyPattern pattern("users##{group}##{id}##{attr}");

    EXPECT_TRUE(pattern.has_capture("group"));
    EXPECT_TRUE(pattern.has_capture("id"));
    EXPECT_FALSE(pattern.has_capture("attr"));  // attr is special
    EXPECT_FALSE(pattern.has_capture("nonexistent"));
}

TEST_F(KeyPatternTest, CaptureIndex) {
    KeyPattern pattern("users##{group}##{id}##{attr}");

    EXPECT_EQ(pattern.capture_index("group"), 0);
    EXPECT_EQ(pattern.capture_index("id"), 1);
    EXPECT_EQ(pattern.capture_index("nonexistent"), -1);
}

TEST_F(KeyPatternTest, ErrorEmptyPattern) {
    EXPECT_THROW(KeyPattern(""), KeyPatternError);
}

TEST_F(KeyPatternTest, ErrorNoAttr) {
    EXPECT_THROW(KeyPattern("users##{group}##{id}"), KeyPatternError);
}

TEST_F(KeyPatternTest, ErrorUnclosedBrace) {
    EXPECT_THROW(KeyPattern("users##{group##{attr}"), KeyPatternError);
}

TEST_F(KeyPatternTest, ErrorEmptyPlaceholder) {
    EXPECT_THROW(KeyPattern("users##{}##{attr}"), KeyPatternError);
}

TEST_F(KeyPatternTest, ErrorMultipleAttr) {
    EXPECT_THROW(KeyPattern("users##{attr}##{attr}"), KeyPatternError);
}

TEST_F(KeyPatternTest, ErrorDuplicateCapture) {
    EXPECT_THROW(KeyPattern("users##{id}##{id}##{attr}"), KeyPatternError);
}

TEST_F(KeyPatternTest, ErrorConsecutiveVariables) {
    EXPECT_THROW(KeyPattern("users##{group}{id}##{attr}"), KeyPatternError);
}

TEST_F(KeyPatternTest, ErrorInvalidCharInName) {
    EXPECT_THROW(KeyPattern("users##{group-name}##{attr}"), KeyPatternError);
}
