CREATE OR REPLACE PROCEDURE `project-name.snapshot_dataset.sp_dynamic_snapshots_bigquery`()
BEGIN
DECLARE log_date string;
DECLARE custom_message string;
DECLARE v_project_name string;
DECLARE v_dataset_name string;
DECLARE v_table_name string;
DECLARE v_CSSTifnotexists_query string;
DECLARE v_CSStablename string;
DECLARE v_droptable_query string;
DECLARE v_csst_query string;

    BEGIN
	SET custom_message ='Error during snapshot creation';
	
	/*
	URL : https://medium.com/google-cloud/creating-bigquery-table-snapshots-dynamically-c3d14ccd368a
	*/

	FOR source_tables IN
      (
	  
		/* 
		Selecting the project_name,dataset_name and table name dynamically from INFORMATION_SCHEMA whose snapshots we need to create	
		*/
		SELECT
			table_catalog AS project_name,
			table_schema AS dataset_name,
			table_name
		FROM
		`project-name.manual_input.INFORMATION_SCHEMA.TABLES`
			WHERE
			table_type = 'BASE TABLE'
			AND table_name NOT IN('error_log_table')
    
		/* 
		-> Filtering out the table(s) we do not need a snapshot of.
		-> This filtering is optional. You can choose not to use the filter
		*/
		
      )
    DO
  
      SET v_project_name = source_tables.project_name;
      SET v_dataset_name = source_tables.dataset_name;
      SET v_table_name = source_tables.table_name;
      SET v_CSStablename = source_tables.table_name || '_tss';
   
		/* 
		-> During the initial run, the snapshots tables do not exist. So we need to create them by default.
		-> v_CSS means variable CreateSnapShot
		-> All snapshots will be created in the snapshot_dataset and will have '_tss' postfix attached to signify a snapshot table
		*/
   
		SET v_CSSTifnotexists_query = FORMAT("""CREATE SNAPSHOT TABLE IF NOT EXISTS `%s.%s.%s` CLONE `%s.%s.%s`""",
		v_project_name,
		'snapshot_dataset',
		v_CSStablename,
		v_project_name,
		v_dataset_name,
		v_table_name);
   
    
		/* 
		Dropping the old snaphot as "Create or replace table" don't work for snapshot tables 
		*/
		
		SET v_droptable_query = FORMAT("""drop snapshot table `%s.%s.%s`;""",v_project_name,'snapshot_dataset',v_CSStablename);
  
  
		/* 
		Creating a snaphot table with 2 days of expiration period i.e the table will be dropped after two days.		
		*/
	
		SET v_csst_query = FORMAT("""CREATE SNAPSHOT TABLE `%s.%s.%s` CLONE `%s.%s.%s` OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 2 DAY));""",
		
		v_project_name,
		'snapshot_dataset',
		v_CSStablename,
		v_project_name,
		v_dataset_name,
		v_table_name);
		
  
      EXECUTE IMMEDIATE v_CSSTifnotexists_query;    
      EXECUTE IMMEDIATE v_droptable_query;
      EXECUTE IMMEDIATE v_csst_query;

  END FOR;
                
    EXCEPTION WHEN ERROR THEN 

    SET LOG_DATE=( SELECT CAST(CURRENT_DATETIME() AS STRING) AS LOG_DATE); 
    
    
    EXECUTE IMMEDIATE "insert into `project-name.error_logging_dataset.error_log_table` (LOG_DATE, PROJECT_NAME, DATASET_NAME, PROCEDURE_NAME,ERROR_STATEMENT_TEXT, ERROR_MESSAGE, CUSTOM_MESSAGE) values (?,?,?,?,?,?,?)" USING LOG_DATE,'project-name','snapshot_dataset','sp_dynamic_snapshots_bigquery',@@error.statement_text,@@error.message,custom_message;
    
        END;
END;