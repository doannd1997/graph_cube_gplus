use gplus
GO

IF OBJECT_ID(N'dbo.user_profile', N'U') IS NOT NULL
	DROP TABLE user_profile
GO 

CREATE TABLE [user_profile] (
	[indexed_id] VARCHAR(50) NOT NULL PRIMARY KEY,
	[gender] VARCHAR(5),
	[job_title] VARCHAR(200),
	[place] VARCHAR(200),
	[university] VARCHAR(200),
	[institution] VARCHAR(200),
)
GO

IF OBJECT_ID(N'dbo.follow', N'U') IS NOT NULL
	DROP TABLE follow
GO

CREATE TABLE follow (
	[follow_id] INT IDENTITY(1, 1) NOT NULL PRIMARY KEY,
	[start_id] VARCHAR(50) REFERENCES [user_profile](indexed_id),
	[end_id] VARCHAR(50) REFERENCES [user_profile](indexed_id)
)
GO