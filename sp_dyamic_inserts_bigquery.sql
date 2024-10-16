CREATE OR REPLACE PROCEDURE `project-name.prod_dataset.sp_dyamic_inserts_bigquery`()
BEGIN
DECLARE log_date string;
DECLARE custom_message string;
DECLARE v_project_name string;
DECLARE v_dataset_name string;
DECLARE v_table_name string;
DECLARE v_truncate_query string;
DECLARE final_query string;

 BEGIN
 SET custom_message ='Error during dynamic inserts';
 
 /*
 URL : https://medium.com/@sahaabhik9/dynamic-inserts-in-google-bigquery-88b8ff9d2671
 */

 FOR source_tables IN
 (
		/*
		Sample Source Tables :
		The source dataset is 'manual_input'
		1) `project-name.manual_input.all_orders_table`
		2) `project-name.manual_input.hockey_data`
		
		Sample Destination Tables :
		The source dataset is 'prod_dataset'
		1) `project-name.prod_dataset.all_orders_table`
		2) `project-name.prod_dataset.hockey_data`
		
		The source tables in both the datasets have identical schemas. 
		*/
		
	with base_tables AS
	(
		select 'project-name' as project_name,'manual_input' as dataset_name,'all_orders_table' as table_name union all
		select 'project-name' as project_name,'manual_input' as dataset_name,'hockey_data' as table_name
	)
		select project_name,dataset_name,table_name from base_tables 
 )
	DO
 
		SET v_project_name = source_tables.project_name;
		SET v_dataset_name = source_tables.dataset_name;
		SET v_table_name = source_tables.table_name;
		
		/*
		-> Truncating the destination tables in 'prod_dataset' one table at a time before inserting records to avoid duplicate
			records in destination tables.
		-> You can choose to skip this step.
		*/
		SET v_truncate_query = FORMAT("""TRUNCATE TABLE `%s.%s.%s`;""",v_project_name,'prod_dataset',v_table_name);
 
		/*
		-> Printing the query for checking		
		*/
		select v_truncate_query;
 
 
  
		SET final_query = FORMAT("""INSERT into `%s.%s.%s` (%s) select %s from `%s.%s.%s`;""",		
		/*
		-> Setting up the variables for the destination dataset (prod_dataset) tables 
		*/
		v_project_name,--1
		'prod_dataset',--2
		v_table_name,--3
		
		/*
		-> Specifiying the column names only from destination dataset tables to prevent schema mismatch incase new columns have 
		been added to the source datasets tables.
		*/
		(SELECT STRING_AGG( upper(column_name), ',')
		from(
		SELECT column_name
		FROM 
		`project-name.prod_dataset.INFORMATION_SCHEMA.COLUMNS`
		where table_name =v_table_name
		order by ordinal_position )), --4
 
		/*
		-> Specifiying the column names only from destination dataset tables to prevent schema mismatch incase new columns have 
		been added to the source datasets tables.
		-> The have also been safe casted to the destination table column types to prevent datatype column mismatches.	
		*/		
		(SELECT STRING_AGG( upper(column_name), ',')
		from(
		SELECT concat('SAFE_CAST(',column_name,' AS ',data_type, ') AS ',column_name) as column_name
		FROM 
		`project-name.prod_dataset.INFORMATION_SCHEMA.COLUMNS`
		where table_name =v_table_name
		order by ordinal_position )), --5

		/*
		-> Setting up the variables for the source dataset (manual_input) tables 
		*/		
		v_project_name,
		v_dataset_name,
		v_table_name);
		
		/*
		-> Printing the query for checking		
		*/ 
		select final_query;
 
 
	EXECUTE IMMEDIATE v_truncate_query; 
	EXECUTE IMMEDIATE final_query;

 END FOR;
 
 EXCEPTION WHEN ERROR THEN 

 SET LOG_DATE=( SELECT CAST(CURRENT_DATETIME() AS STRING) AS LOG_DATE); 
 
 
 EXECUTE IMMEDIATE "insert into `project-name.error_logging_dataset.error_log_table` (LOG_DATE, PROJECT_NAME, DATASET_NAME, PROCEDURE_NAME,ERROR_STATEMENT_TEXT, ERROR_MESSAGE, CUSTOM_MESSAGE) values (?,?,?,?,?,?,?)" USING LOG_DATE,'project-name','prod_dataset','sp_dyamic_inserts_bigquery',@@error.statement_text,@@error.message,custom_message;
 
 END;
END;