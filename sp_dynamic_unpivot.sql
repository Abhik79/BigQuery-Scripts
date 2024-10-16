CREATE OR REPLACE PROCEDURE `project-name.dataset_name.sp_dynamic_unpivot`()
BEGIN
DECLARE log_date string;
DECLARE custom_message string;
DECLARE myunpivot string;
	
	/*
	URL : https://medium.com/towardsdev/part-1-dynamic-unpivoting-in-bigquery-db7dc8c6ff3
	*/
	
    BEGIN
        SET custom_message ='Error while selecting data from INFORMATION_SCHEMA.COLUMNS';

        SET myunpivot=(SELECT
        CONCAT('(', STRING_AGG(column_name,','),')'),
        FROM (
        SELECT
         column_name
        FROM
         `project-name.dataset_name.INFORMATION_SCHEMA.COLUMNS`
        WHERE
         table_name='passenger_data_nz_pivoted'
         AND column_name NOT IN('port','year'))
       );
    
   SET custom_message ='Error while creating passenger_data_nz_unpivoted table';    
         
   EXECUTE IMMEDIATE format("""create or replace table `project-name.dataset_name.passenger_data_nz_unpivoted` AS
    select
     port,
     year,
     people_count,
     citizenship
     from `project-name.dataset_name.passenger_data_nz_pivoted`
    unpivot
    (
    people_count
    FOR citizenship in %s
    )""",myunpivot);
    
    
    EXCEPTION WHEN ERROR THEN 

    SET LOG_DATE=( SELECT CAST(CURRENT_DATETIME() AS STRING) AS LOG_DATE); 
    
    
    EXECUTE IMMEDIATE "insert into `project-name.error_logging_dataset.error_log_table` (LOG_DATE, PROJECT_NAME, DATASET_NAME, PROCEDURE_NAME,ERROR_STATEMENT_TEXT, ERROR_MESSAGE, CUSTOM_MESSAGE) values 
    (?,?,?,?,?,?,?)" USING LOG_DATE,'project-name','dataset_name','sp_dynamic_unpivot',@@error.statement_text,@@error.message,custom_message;
    
        END;
END;