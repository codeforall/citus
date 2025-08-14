--
-- Test that background jobs can be configured with GUCs
--
CREATE TABLE test_job_config_results (result text);

-- Create a job that will show the application_name
SELECT citus_create_background_job('test', 'test job') \gset

-- Schedule a task with a custom application_name
SELECT schedule_background_task(
	:'job_id',
	'INSERT INTO test_job_config_results (SELECT current_setting($$application_name$$));',
	ARRAY[ROW('application_name', 'test_app_name', true)]::pg_catalog.job_config_option[]
);

-- Wait for the job to finish
SELECT citus_job_wait(:'job_id');

-- Check the results
SELECT * FROM test_job_config_results;

-- Clean up
DROP TABLE test_job_config_results;
TRUNCATE pg_dist_background_job CASCADE;
TRUNCATE pg_dist_background_task_depend;
TRUNCATE pg_dist_background_task;
