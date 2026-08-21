CREATE TABLE [dbo].[studentpreferences] (

	[student_id] varchar(50) NOT NULL, 
	[preference_key] varchar(100) NOT NULL, 
	[preference_value] varchar(1000) NULL, 
	[is_active] bit NOT NULL, 
	[created_at_utc] datetime2(6) NOT NULL, 
	[created_by_upn] varchar(320) NOT NULL, 
	[updated_at_utc] datetime2(6) NOT NULL, 
	[updated_by_upn] varchar(320) NOT NULL, 
	[version_number] bigint NOT NULL
);