USE [WH_Control]
GO

/****** Object:  View [Log].[Summary]    Script Date: 06/02/2023 15:08:18 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE VIEW [Log].[Summary]
AS

SELECT	CONVERT(varchar,LoadDate)+CASE WHEN Run > 1 AND MAX(CASE Step WHEN 'Dates' THEN 0 ELSE RowsProcessed END) = 0 THEN ' (Waiting)' ELSE '' END Load, 
		LoadDate, 
		[Procedure],
		Run,
		MIN(StartDateTime) StartDateTime, 
		MAX(EndDateTime) EndDateTime, 
		CONVERT(time(0),DATEADD(s,DATEDIFF(s,min(StartDateTime),max(EndDateTime)), '00:00:00.000')) Elapsed,
		MIN(Status) Status,
		MAX(CASE WHEN Step LIKE '%-Prepare' THEN RowsProcessed WHEN Step LIKE '%Prepare' THEN RowsProcessed ELSE 0 END) RowsProcessed, 
		SUM(CASE WHEN Step LIKE '%Insert' THEN RowsProcessed WHEN Step LIKE '%-Switch-New' THEN RowsProcessed ELSE 0 END) RowsInserted, 
		SUM(CASE WHEN Step LIKE '%Update' THEN RowsProcessed ELSE 0 END) RowsUpdated, 
		SUM(CASE WHEN Step LIKE '%Delete' THEN RowsProcessed ELSE 0 END) RowsDeleted, 
		MAX(CASE Status WHEN 'Running' THEN Step ELSE NULL END) InProgress, 
		MAX(CASE WHEN Step = 'Dates' THEN Message ELSE '' END) Dates
FROM	[Log].[Log] 
GROUP BY LoadDate, Run, [Procedure]
GO


