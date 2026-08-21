-- Auto Generated (Do not modify) 588E551E41DFC7CEBBC25BEB8449FA655D8917923C8BD7C73E5D6403F41F6A25
-- Step 1: Since there is no view named [vw_studentssecurity_enrollments] in the schema,
-- we will assume the user refers to a custom view that joins [studentssecurity] and [enrollments] (and possibly [students]).
-- The request is to remove the join to [students] and ensure all VARCHAR columns are cast to VARCHAR(255).
-- We'll create a new view [vw_studentssecurity_enrollments2] as a safe variant.

-- Step 2: Identify all VARCHAR columns in [studentssecurity] and [enrollments]:
-- [studentssecurity]: UserUPN (VARCHAR), student_id (VARCHAR)
-- [enrollments]: term_id, student_id, crn, course_id, section, subject, catalog_nbr, course_title, credits, enrollment_status, grade, quality_points, _source_table, term_id_crn (all VARCHAR)

-- Step 3: Remove any join to [students] and only join [studentssecurity] to [enrollments] on student_id.

-- Step 4: In the SELECT, cast all VARCHAR columns to VARCHAR(255).

CREATE VIEW [dbo].[vw_enrollments] 
WITH SCHEMABINDING
AS
SELECT
    CAST([ss].[UserUPN] AS VARCHAR(255)) AS [UserUPN],
    CAST([ss].[student_id] AS VARCHAR(255)) AS [student_id],
    CAST([e].[term_id] AS VARCHAR(255)) AS [term_id],
    CAST([e].[student_id] AS VARCHAR(255)) AS [enrollment_student_id],
    CAST([e].[crn] AS VARCHAR(255)) AS [crn],
    CAST([e].[course_id] AS VARCHAR(255)) AS [course_id],
    CAST([e].[section] AS VARCHAR(255)) AS [section],
    CAST([e].[subject] AS VARCHAR(255)) AS [subject],
    CAST([e].[catalog_nbr] AS VARCHAR(255)) AS [catalog_nbr],
    CAST([e].[course_title] AS VARCHAR(255)) AS [course_title],
    CAST([e].[credits] AS VARCHAR(255)) AS [credits],
    CAST([e].[enrollment_status] AS VARCHAR(255)) AS [enrollment_status],
    CAST([e].[grade] AS VARCHAR(255)) AS [grade],
    CAST([e].[quality_points] AS VARCHAR(255)) AS [quality_points],
    [e].[_ingest_ts],
    CAST([e].[_source_table] AS VARCHAR(255)) AS [_source_table],
    CAST([e].[term_id_crn] AS VARCHAR(255)) AS [term_id_crn]
FROM
    [dbo].[studentssecurity] AS [ss]
    INNER JOIN [dbo].[enrollments] AS [e]
        ON [ss].[student_id] = [e].[student_id];