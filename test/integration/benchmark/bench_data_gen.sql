-- Generate benchmark test data
-- Uses :bench_size variable for row count (passed via psql -v)
-- Run with: psql -v bench_size=1000 -f bench_data_gen.sql

-- Use a default if bench_size not set
\set bench_size_default 1000
SELECT COALESCE(:'bench_size', '1000')::int AS actual_size \gset

-- Generate users across 10 tenants
-- Each user has name, email, status attributes
INSERT INTO bench_users (tenant, user_id, name, email, status)
SELECT
    'tenant_' || (i % 10),
    'user_' || i,
    'User Name ' || i,
    'user' || i || '@example.com',
    CASE WHEN i % 3 = 0 THEN 'active' WHEN i % 3 = 1 THEN 'inactive' ELSE 'pending' END
FROM generate_series(1, :actual_size) AS i;

-- Generate metrics across 5 tenants × 20 services
-- Each metric has value and timestamp
INSERT INTO bench_metrics (tenant, service, metric_id, value, timestamp)
SELECT
    'tenant_' || (i % 5),
    'service_' || (i % 20),
    'metric_' || i,
    (random() * 1000)::int::text,
    now()::text
FROM generate_series(1, :actual_size) AS i;

-- Events table is used for insert benchmarks, so we don't pre-populate it
-- (it gets populated during the benchmark itself)

SELECT 'Generated ' || :actual_size || ' rows in bench_users and bench_metrics' AS status;
