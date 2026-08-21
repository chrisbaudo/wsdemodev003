-- Auto Generated (Do not modify) 4F1A9324F143FB5BEA0B19C37B1157EEF1D34B0FFA6EDD95A3D1759B268139D0


CREATE VIEW [dbo].[vw_courses] 
WITH SCHEMABINDING
AS
SELECT
    CAST([ss].[UserUPN] AS VARCHAR(255)) AS [UserUPN],
    CAST([ss].[student_id] AS VARCHAR(255)) AS [student_id],
    CAST([c].[term_id] AS VARCHAR(255)) AS [term_id],
    CAST([c].[course_id] AS VARCHAR(255)) AS [course_id],
    CAST([c].[section] AS VARCHAR(255)) AS [section],
    CAST([c].[crn] AS VARCHAR(255)) AS [crn],
    CAST([c].[subject] AS VARCHAR(255)) AS [subject],
    CAST([c].[catalog_nbr] AS VARCHAR(255)) AS [catalog_nbr],
    CAST([c].[course_title] AS VARCHAR(255)) AS [course_title],
    CAST([c].[credits] AS VARCHAR(255)) AS [credits],
    CAST([c].[instructor] AS VARCHAR(255)) AS [instructor],
    CAST([c].[max_enrollment] AS VARCHAR(255)) AS [max_enrollment],
    [c].[_ingest_ts],
    CAST([c].[_source_table] AS VARCHAR(255)) AS [_source_table],
    CAST([c].[term_id_crn] AS VARCHAR(255)) AS [term_id_crn]
FROM [dbo].[studentssecurity] AS [ss]
CROSS JOIN [dbo].[courses] c;