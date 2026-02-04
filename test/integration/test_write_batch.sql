-- Test WriteBatch functionality for level_pivot FDW
-- These tests verify that batch operations use LevelDB's WriteBatch
-- for atomic writes

-- Setup: Clean state
DELETE FROM users WHERE group_name IN ('wb_test', 'wb_atomic');

-- ============================================
-- Test 1: Multi-row INSERT uses WriteBatch
-- ============================================
SELECT '=== Test 1: Multi-row INSERT with WriteBatch ===' AS test;

-- Insert multiple rows in a single statement (should use WriteBatch)
INSERT INTO users (group_name, id, name, email)
VALUES
    ('wb_test', 'user1', 'Write Batch User 1', 'wb1@test.com'),
    ('wb_test', 'user2', 'Write Batch User 2', 'wb2@test.com'),
    ('wb_test', 'user3', 'Write Batch User 3', 'wb3@test.com'),
    ('wb_test', 'user4', 'Write Batch User 4', 'wb4@test.com'),
    ('wb_test', 'user5', 'Write Batch User 5', 'wb5@test.com');

-- Verify all rows were inserted
SELECT COUNT(*) AS inserted_count FROM users WHERE group_name = 'wb_test';

-- Verify data integrity
SELECT * FROM users WHERE group_name = 'wb_test' ORDER BY id;

-- ============================================
-- Test 2: Multi-row UPDATE uses WriteBatch
-- ============================================
SELECT '=== Test 2: Multi-row UPDATE with WriteBatch ===' AS test;

-- Update multiple rows (should use WriteBatch)
UPDATE users SET email = 'updated@test.com'
WHERE group_name = 'wb_test' AND id IN ('user1', 'user2', 'user3');

-- Verify updates
SELECT id, email FROM users
WHERE group_name = 'wb_test'
ORDER BY id;

-- ============================================
-- Test 3: Multi-row DELETE uses WriteBatch
-- ============================================
SELECT '=== Test 3: Multi-row DELETE with WriteBatch ===' AS test;

-- Delete multiple rows (should use WriteBatch)
DELETE FROM users
WHERE group_name = 'wb_test' AND id IN ('user4', 'user5');

-- Verify deletes
SELECT COUNT(*) AS remaining_count FROM users WHERE group_name = 'wb_test';

-- ============================================
-- Test 4: INSERT with many attributes
-- ============================================
SELECT '=== Test 4: INSERT with all attributes ===' AS test;

-- Insert a row with all available attributes
INSERT INTO users (group_name, id, name, email, created_at)
VALUES ('wb_test', 'full_user', 'Full User', 'full@test.com', '2024-01-15');

SELECT * FROM users WHERE group_name = 'wb_test' AND id = 'full_user';

-- ============================================
-- Test 5: Large batch INSERT
-- ============================================
SELECT '=== Test 5: Large batch INSERT ===' AS test;

-- Insert a larger batch to stress test WriteBatch
INSERT INTO users (group_name, id, name, email)
SELECT
    'wb_atomic',
    'user' || generate_series,
    'Atomic User ' || generate_series,
    'atomic' || generate_series || '@test.com'
FROM generate_series(1, 50);

-- Verify all 50 rows were inserted atomically
SELECT COUNT(*) AS large_batch_count FROM users WHERE group_name = 'wb_atomic';

-- ============================================
-- Test 6: UPDATE then DELETE in sequence
-- ============================================
SELECT '=== Test 6: UPDATE then DELETE sequence ===' AS test;

-- First update some rows
UPDATE users SET name = 'Modified User'
WHERE group_name = 'wb_atomic' AND id LIKE 'user1%';

-- Count modified rows
SELECT COUNT(*) AS modified_count
FROM users
WHERE group_name = 'wb_atomic' AND name = 'Modified User';

-- Then delete them
DELETE FROM users
WHERE group_name = 'wb_atomic' AND name = 'Modified User';

-- Verify deletion
SELECT COUNT(*) AS after_delete_count FROM users WHERE group_name = 'wb_atomic';

-- ============================================
-- Cleanup
-- ============================================
SELECT '=== Cleanup ===' AS test;

DELETE FROM users WHERE group_name IN ('wb_test', 'wb_atomic');

-- Verify cleanup
SELECT COUNT(*) AS final_count
FROM users
WHERE group_name IN ('wb_test', 'wb_atomic');

SELECT 'WriteBatch tests completed successfully' AS status;
