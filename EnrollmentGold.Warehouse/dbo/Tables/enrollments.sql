CREATE TABLE [dbo].[enrollments] (

	[term_id] varchar(max) NULL, 
	[student_id] varchar(max) NULL, 
	[crn] varchar(max) NULL, 
	[course_id] varchar(max) NULL, 
	[section] varchar(max) NULL, 
	[subject] varchar(max) NULL, 
	[catalog_nbr] varchar(max) NULL, 
	[course_title] varchar(max) NULL, 
	[credits] varchar(max) NULL, 
	[enrollment_status] varchar(max) NULL, 
	[grade] varchar(max) NULL, 
	[quality_points] varchar(max) NULL, 
	[_ingest_ts] datetime2(6) NOT NULL, 
	[_source_table] varchar(max) NOT NULL
);