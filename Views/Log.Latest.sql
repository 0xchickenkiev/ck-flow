USE [WH_Control]
GO

/****** Object:  View [Log].[Latest]    Script Date: 06/02/2023 15:08:03 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE VIEW [Log].[Latest]
AS
WITH 
	Candidate as 
	(	SELECT	TOP 1000 LoadDate, [Procedure], Run, MIN(StartDateTime) StartDateTime
		FROM	Log.Log
		GROUP BY LoadDate, [Procedure], Run
		ORDER BY 4 DESC
	)
SELECT	TOP 1000 s.* 
FROM	Candidate c 
		INNER JOIN Log.Summary s ON s.LoadDate = c.LoadDate and s.[Procedure] = c.[Procedure] and s.Run = c.Run
ORDER BY s.StartDateTime DESC
GO


