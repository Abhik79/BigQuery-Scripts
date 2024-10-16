# BigQuery Stored Procedures Documentation

## Overview

This repository contains three BigQuery stored procedures that automate various data management tasks:
1. `sp_dyamic_inserts_bigquery`: Automates the dynamic insertion of data from one dataset to another.
2. `sp_dynamic_snapshots_bigquery`: Creates and manages snapshots of tables dynamically.
3. `sp_dynamic_unpivot`: Automates the unpivoting process for a specified table.

Each stored procedure includes error handling and logs errors to a designated `error_log_table` in `error_logging_dataset`.

## Procedure Details

### Procedure 1: Dynamic Inserts

**Name:** `sp_dyamic_inserts_bigquery`  
**Location:** `project-name.prod_dataset.sp_dyamic_inserts_bigquery`

This procedure performs the following:
- Truncates destination tables before inserting records to prevent duplicates.
- Inserts data from source tables in `manual_input` dataset to destination tables in `prod_dataset`.
- Ensures schema consistency by selecting columns only from destination tables.

**How to Call:**
```sql
CALL `project-name.prod_dataset.sp_dyamic_inserts_bigquery`();
```

### Procedure 2: Dynamic Snapshots

**Name:** `sp_dynamic_snapshots_bigquery`  
**Location:** `project-name.snapshot_dataset.sp_dynamic_snapshots_bigquery`

This procedure performs the following:
- Creates snapshot tables dynamically for all base tables in `manual_input` dataset.
- Drops older snapshot tables before creating new ones.
- Sets a two-day expiration period for snapshot tables to automatically remove them after two days.

**How to Call:**
```sql
CALL `project-name.snapshot_dataset.sp_dynamic_snapshots_bigquery`();
```

### Procedure 3: Dynamic Unpivot

**Name:** `sp_dynamic_unpivot`  
**Location:** `project-name.dataset_name.sp_dynamic_unpivot`

This procedure performs the following:
- Creates an unpivoted table `passenger_data_nz_unpivoted` from the pivoted table `passenger_data_nz_pivoted`.
- The unpivoting is done dynamically based on columns from `passenger_data_nz_pivoted` except `port` and `year`.

**How to Call:**
```sql
CALL `project-name.dataset_name.sp_dynamic_unpivot`();
```

## Error Handling

All procedures include error handling logic to capture errors during execution. In case of an error:
- The error details (timestamp, project name, dataset name, procedure name, error statement, error message, and custom message) are logged into `error_logging_dataset.error_log_table`.

## Reference Links

1. [Dynamic Inserts in BigQuery](https://medium.com/@sahaabhik9/dynamic-inserts-in-google-bigquery-88b8ff9d2671)
2. [Creating BigQuery Table Snapshots Dynamically](https://medium.com/google-cloud/creating-bigquery-table-snapshots-dynamically-c3d14ccd368a)
3. [Dynamic Unpivoting in BigQuery](https://medium.com/towardsdev/part-1-dynamic-unpivoting-in-bigquery-db7dc8c6ff3)

---

This README provides an overview of each procedure, details on their purpose, and instructions on how to execute them. Make sure to replace `project-name` and `dataset_name` with the appropriate values in your BigQuery environment before use.