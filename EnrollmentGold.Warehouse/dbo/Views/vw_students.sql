-- Auto Generated (Do not modify) 34634186B3C602BA5620D17C4DF331CBB93813411D54A580C21BD46B36835570
CREATE   VIEW [dbo].[vw_students] 
WITH SCHEMABINDING
AS
SELECT
    CAST([s].[student_id] AS VARCHAR(255)) AS [student_id],
    CAST([s].[first_name] AS VARCHAR(255)) AS [first_name],
    CAST([s].[last_name] AS VARCHAR(255)) AS [last_name],
    [s].[date_of_birth],
    CAST([s].[admit_term] AS VARCHAR(255)) AS [admit_term],
    CAST([s].[major] AS VARCHAR(255)) AS [major],
    CAST([s].[class_level] AS VARCHAR(255)) AS [class_level],
    CAST([s].[residency] AS VARCHAR(255)) AS [residency],
    CAST([s].[pell_eligible] AS VARCHAR(255)) AS [pell_eligible],
    CAST([s].[cumulative_gpa] AS VARCHAR(255)) AS [cumulative_gpa],
    [s].[birth_year],
    [s].[birth_month],
    [s].[_ingest_ts],
    CAST([s].[_source_table] AS VARCHAR(255)) AS [_source_table],
    CAST([ss].[UserUPN] AS VARCHAR(255)) AS [UserUPN]
FROM [dbo].[students] AS [s]
INNER JOIN [dbo].[studentssecurity] AS [ss]
    ON [s].[student_id] = [ss].[student_id];