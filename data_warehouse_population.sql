\i './SA_ONLINE/ext_online_retail_sales.sql'
\i './SA_OFFLINE/ext_offline_retail_sales.sql'

SET max_parallel_workers = 8;
SET max_parallel_maintenance_workers = 8;
SET max_parallel_workers_per_gather = 8;
SET parallel_leader_participation = off;
SET parallel_tuple_cost = 0; 
SET parallel_setup_cost = 0;
SET min_parallel_table_scan_size = 0;

CALL BL_CL.SP_CL_MASTER_PROCEDURE();