CREATE TABLE [dbo].[terms] (

	[term_id] varchar(max) NULL, 
	[term_name] varchar(max) NULL, 
	[start_date] varchar(max) NULL, 
	[end_date] varchar(max) NULL, 
	[instruction_weeks] varchar(max) NULL, 
	[_ingest_ts] datetime2(6) NOT NULL, 
	[_source_table] varchar(max) NOT NULL
);