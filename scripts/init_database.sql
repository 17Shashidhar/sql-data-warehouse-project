/*
=============================================================
Create Database and Schemas
=============================================================

Script Purpose:
    This script creates a new database named 'DataWarehouse'
    after checking if it already exists.

    If the database exists, it is dropped and recreated.
    Additionally, the script sets up three schemas within
    the database:
        - bronze  (Raw data layer)
        - silver  (Cleaned and transformed data layer)
        - gold    (Business and analytics layer)

WARNING:
    Running this script will drop the entire 'DataWarehouse'
    database if it exists.

    All data in the database will be permanently deleted.
    Proceed with caution and ensure you have proper backups
    before running this script.


-------------------------------------------------------------
-- Switch to master database
-------------------------------------------------------------
USE master;
GO

-------------------------------------------------------------
-- Drop and recreate DataWarehouse database if it exists
-------------------------------------------------------------
IF EXISTS (
    SELECT 1
    FROM sys.databases
    WHERE name = 'DataWarehouse'
)
BEGIN
    ALTER DATABASE DataWarehouse
    SET SINGLE_USER
    WITH ROLLBACK IMMEDIATE;

    DROP DATABASE DataWarehouse;
END;
GO

-------------------------------------------------------------
-- Create DataWarehouse database
-------------------------------------------------------------
CREATE DATABASE DataWarehouse;
GO

-------------------------------------------------------------
-- Switch to DataWarehouse database
-------------------------------------------------------------
USE DataWarehouse;
GO

-------------------------------------------------------------
-- Create Schemas (Medallion Architecture)
-------------------------------------------------------------

-- Bronze schema: Raw source data
CREATE SCHEMA bronze;
GO

-- Silver schema: Cleaned and transformed data
CREATE SCHEMA silver;
GO

-- Gold schema: Business-ready analytics data
CREATE SCHEMA gold;
GO

-------------------------------------------------------------
-- Verify schema creation
-------------------------------------------------------------
SELECT name AS schema_name
FROM sys.schemas
WHERE name IN ('bronze', 'silver', 'gold');
GO
