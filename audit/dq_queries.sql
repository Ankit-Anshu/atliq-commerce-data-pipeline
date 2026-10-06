SELECT run_date, table_name, rule, rows_affected
FROM atliq.silver.dq_log
ORDER BY run_date, table_name, rule;


SELECT table_name, rule, SUM(rows_affected) AS rows_affected
FROM atliq.silver.dq_log
GROUP BY table_name, rule
ORDER BY table_name, rule;