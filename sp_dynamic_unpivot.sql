CREATE OR REPLACE PROCEDURE `project-name.dataset_name.sp_dynamic_pivot`()
BEGIN
DECLARE log_date string;
DECLARE custom_message string;
DECLARE mypivot string;

    BEGIN
                SET custom_message ='Error while creating all_orders_table';

             set mypivot=(SELECT
								CONCAT('(', STRING_AGG(column_name,','),')'),
								FROM (
								SELECT
									DISTINCT 
									'"' || 
									REGEXP_REPLACE(
										REGEXP_REPLACE(
										REGEXP_REPLACE(
											REGEXP_REPLACE(
											LOWER(citizenship), 
											'[,]', ''  -- Replace commas with nulls
											),
											' ', '_'    -- Replace spaces with underscores
										),
										'[(]', ''     -- Replace opening parentheses with nulls
										),
										'[)]', ''     -- Replace closing parentheses with nulls
									) || '"' AS column_name
									FROM
									`project-name.dataset_name.passenger_data_nz_unpivoted`)
							);
									
									
			EXECUTE IMMEDIATE format("""
				create or replace table `project-name.dataset_name.passenger_data_nz_pivoted` as
				select 
				*
				from `project-name.dataset_name.passenger_data_nz_unpivoted`
				PIVOT
				(
				sum(total_count)
				FOR REGEXP_REPLACE(
										REGEXP_REPLACE(
										REGEXP_REPLACE(
											REGEXP_REPLACE(
											LOWER(citizenship), 
											'[,]', ''  -- Replace commas with nulls
											),
											' ', '_'    -- Replace spaces with underscores
										),
										'[(]', ''     -- Replace opening parentheses with nulls
										),
										'[)]', ''     -- Replace closing parentheses with nulls
									)
				IN %s
				)	""",mypivot);
				
				
    EXCEPTION WHEN ERROR THEN 

    SET LOG_DATE=( SELECT CAST(CURRENT_DATETIME() AS STRING) AS LOG_DATE); 
    
    
    EXECUTE IMMEDIATE "insert into `project-name.dataset_name.error_log_table` (LOG_DATE, PROJECT_NAME, DATASET_NAME, PROCEDURE_NAME,ERROR_STATEMENT_TEXT, ERROR_MESSAGE, CUSTOM_MESSAGE) values (?,?,?,?,?,?,?)" USING LOG_DATE,'project-name.dataset_name','manual_input','sp_dynamic_pivot',@@error.statement_text,@@error.message,custom_message;
    
        END;
END;
