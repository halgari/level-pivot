-- Cleanup benchmark data between runs
-- Deletes all data from benchmark tables without dropping them

-- Delete all users
DELETE FROM bench_users;

-- Delete all metrics
DELETE FROM bench_metrics;

-- Delete all events
DELETE FROM bench_events;

-- Clean up LevelDB directory to ensure fresh state
-- This is done via shell, but we confirm tables are empty here
SELECT
    'bench_users: ' || COUNT(*)::text AS users_remaining,
    (SELECT 'bench_metrics: ' || COUNT(*)::text FROM bench_metrics) AS metrics_remaining,
    (SELECT 'bench_events: ' || COUNT(*)::text FROM bench_events) AS events_remaining
FROM bench_users;
