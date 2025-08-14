--
-- Test that background jobs can be configured with GUCs
--
CREATE TABLE test_job_config_results (result text);

-- Create a job that will show the application_name
INSERT INTO pg_dist_background_job (job_type, description) VALUES ('test', 'test job');
SELECT job_id FROM pg_dist_background_job WHERE job_type = 'test' \gset

-- Schedule a task with a custom application_name
SELECT pg_catalog.schedule_background_task(
	:'job_id',
	'SELECT pg_catalog.query_to_xml_and_xmlschema($$INSERT INTO test_job_config_results (SELECT application_name)$$, true, false, $$'');',
	ARRAY[ROW('application_name', 'test_app_name', true)]::pg_catalog.job_config_option[]
);

-- Wait for the job to finish
SELECT citus_job_wait(:job_id);

-- Check the results
SELECT * FROM test_job_config_results;

-- Clean up
DROP TABLE test_job_config_results;
TRUNCATE pg_dist_background_job CASCADE;
TRUNCATE pg_dist_background_task_depend;
TRUNCATE pg_dist_background_task;
