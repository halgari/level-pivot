-- Test SELECT operations for level_pivot FDW

-- Test 1: Insert and select from users table
INSERT INTO users (group_name, id, name, email)
VALUES ('admins', 'user001', 'Alice', 'alice@example.com');

INSERT INTO users (group_name, id, name, email)
VALUES ('admins', 'user002', 'Bob', 'bob@example.com');

INSERT INTO users (group_name, id, name, email)
VALUES ('staff', 'user003', 'Charlie', 'charlie@example.com');

-- Select all users
SELECT '=== All Users ===' AS test;
SELECT * FROM users ORDER BY group_name, id;

-- Select with filter
SELECT '=== Admins Only ===' AS test;
SELECT * FROM users WHERE group_name = 'admins' ORDER BY id;

-- Test 2: Insert and select from sales table
INSERT INTO sales (arg, sub_arg, revenue, count)
VALUES ('sales', 'west', '1000000', '42');

INSERT INTO sales (arg, sub_arg, revenue, count, margin)
VALUES ('sales', 'east', '850000', '38', '0.15');

INSERT INTO sales (arg, sub_arg, revenue)
VALUES ('marketing', 'north', '500000');

-- Select all sales
SELECT '=== All Sales ===' AS test;
SELECT * FROM sales ORDER BY arg, sub_arg;

-- Test 3: Insert and select from metrics table
INSERT INTO metrics (tenant, env, service, requests, latency_p99)
VALUES ('acme', 'prod', 'api', '50000', '45');

INSERT INTO metrics (tenant, env, service, requests, latency_p99, error_rate)
VALUES ('acme', 'staging', 'api', '1000', '32', '0.001');

-- Select all metrics
SELECT '=== All Metrics ===' AS test;
SELECT * FROM metrics ORDER BY tenant, env, service;

-- Select with complex filter
SELECT '=== Production Metrics ===' AS test;
SELECT tenant, service, requests, latency_p99
FROM metrics
WHERE env = 'prod';

SELECT 'SELECT tests completed successfully' AS status;
