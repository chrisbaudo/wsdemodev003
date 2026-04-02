CREATE TABLE [dbo].[students] (

	[student_id] varchar(max) NULL, 
	[first_name] varchar(max) NULL, 
	[last_name] varchar(max) NULL, 
	[date_of_birth] date NULL, 
	[admit_term] varchar(max) NULL, 
	[major] varchar(max) NULL, 
	[class_level] varchar(max) NULL, 
	[residency] varchar(max) NULL, 
	[pell_eligible] varchar(max) NULL, 
	[cumulative_gpa] varchar(max) NULL, 
	[birth_year] bigint NULL, 
	[birth_month] bigint NULL, 
	[_ingest_ts] datetime2(6) NOT NULL, 
	[_source_table] varchar(max) NOT NULL
);