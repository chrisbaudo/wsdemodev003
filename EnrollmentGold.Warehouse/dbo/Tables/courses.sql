CREATE TABLE [dbo].[courses] (

	[term_id] varchar(max) NULL, 
	[course_id] varchar(max) NULL, 
	[section] varchar(max) NULL, 
	[crn] varchar(max) NULL, 
	[subject] varchar(max) NULL, 
	[catalog_nbr] varchar(max) NULL, 
	[course_title] varchar(max) NULL, 
	[credits] varchar(max) NULL, 
	[instructor] varchar(max) NULL, 
	[max_enrollment] varchar(max) NULL, 
	[_ingest_ts] datetime2(6) NOT NULL, 
	[_source_table] varchar(max) NOT NULL
);