-- Test INSERT/UPDATE/DELETE operations for level_pivot FDW

-- Setup: Ensure we have clean state
DELETE FROM users WHERE group_name = 'test';

-- Test INSERT
SELECT '=== Test INSERT ===' AS test;

INSERT INTO users (group_name, id, name, email)
VALUES ('test', 'insert001', 'Insert Test', 'insert@test.com');

SELECT * FROM users WHERE group_name = 'test' AND id = 'insert001';

-- Test UPDATE
SELECT '=== Test UPDATE ===' AS test;

UPDATE users SET email = 'updated@test.com'
WHERE group_name = 'test' AND id = 'insert001';

SELECT * FROM users WHERE group_name = 'test' AND id = 'insert001';

-- Test UPDATE with NULL (should delete the attr key)
SELECT '=== Test UPDATE with NULL ===' AS test;

UPDATE users SET email = NULL, name = 'Name Only'
WHERE group_name = 'test' AND id = 'insert001';

SELECT * FROM users WHERE group_name = 'test' AND id = 'insert001';

-- Test DELETE
SELECT '=== Test DELETE ===' AS test;

DELETE FROM users WHERE group_name = 'test' AND id = 'insert001';

-- Should return no rows
SELECT * FROM users WHERE group_name = 'test' AND id = 'insert001';

-- Test batch INSERT
SELECT '=== Test Batch INSERT ===' AS test;

INSERT INTO users (group_name, id, name, email)
VALUES
    ('batch', 'user1', 'Batch User 1', 'batch1@test.com'),
    ('batch', 'user2', 'Batch User 2', 'batch2@test.com'),
    ('batch', 'user3', 'Batch User 3', 'batch3@test.com');

SELECT * FROM users WHERE group_name = 'batch' ORDER BY id;

-- Test batch DELETE
DELETE FROM users WHERE group_name = 'batch';

-- Should return no rows
SELECT COUNT(*) AS batch_count FROM users WHERE group_name = 'batch';

SELECT 'MODIFY tests completed successfully' AS status;
