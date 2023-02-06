USE [WH_Control]
GO

/****** Object:  StoredProcedure [Dimensional].[Upsert]    Script Date: 06/02/2023 15:05:42 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Dimensional].[Upsert]
(
	@TableFrom	varchar(max) = NULL,
	@TableTo	varchar(max) = NULL,
	@DateFrom	datetime2(7) = NULL OUTPUT,
	@DateTo		datetime2(7) = NULL OUTPUT,
	@DryRun		bit = 0
)
AS

	SET NOCOUNT ON

	DECLARE	@Columns varchar(max) = STUFF((SELECT ', a.'+QUOTENAME(name) FROM sys.dm_exec_describe_first_result_set('SELECT * FROM '+@TableTo,NULL,1) WHERE is_computed_column = 0 ORDER BY column_ordinal FOR XML PATH('')),1,2,'')
	DECLARE	@Update varchar(max) = STUFF((SELECT ', '+QUOTENAME(name)+' = b.'+QUOTENAME(name) FROM sys.dm_exec_describe_first_result_set('SELECT * FROM '+@TableTo,NULL,1) WHERE is_part_of_unique_key = 0 AND is_computed_column = 0 ORDER BY column_ordinal FOR XML PATH('')),1,2,'')
	DECLARE	@KeyJoin varchar(max) = STUFF((SELECT ' AND b.'+QUOTENAME(name)+' = a.'+QUOTENAME(name) FROM sys.dm_exec_describe_first_result_set('SELECT * FROM '+@TableTo,NULL,1) WHERE is_part_of_unique_key = 1 ORDER BY column_ordinal FOR XML PATH('')),1,5,'')
	DECLARE	@CandidateKey varchar(128) = (SELECT TOP 1 QUOTENAME(name) FROM sys.dm_exec_describe_first_result_set('SELECT * FROM '+@TableTo,NULL,1) WHERE is_part_of_unique_key = 1 ORDER BY column_ordinal)

	--DECLARE	@SQLDateWatermark nvarchar(MAX) = 'SELECT @DateWatermark = SYSUTCDATETIME()'
	--DECLARE	@SQLDateFrom nvarchar(MAX) = 'SELECT @DateFrom = ISNULL(MAX(UpdateTimestamp),''1900-01-01'') FROM '+@TableTo
	--DECLARE	@SQLDateTo nvarchar(MAX) = 'SELECT @DateTo = ISNULL(MAX(UpdateTimestamp), ''1900-01-01'') FROM '+@TableFrom+' WHERE UpdateTimestamp >= @DateFrom AND UpdateTimestamp <= @DateWatermark' 
	DECLARE @SQL nvarchar(MAX) = 
	'EXEC Log.Logging	@DateLog,@Me,''Candidates'',''Start'',@RowsProcessed=0
	
	-- Getting the primary key (or most significant column of the primary key) for all changed rows and applying that to the subsequent Changes query makes the often complex MAX() logic on UpdateTimestamp in the Live view massively 
	-- more efficient as it drives off the list of keys and then applies the UpdateTimestamp logic before filtering on the date range rather than working out the date logic for a join of all tables unfiltered, then applying the date logic and filters
	SELECT	DISTINCT '+@CandidateKey+'
	INTO	#x
	FROM	'+@TableFrom+'
	WHERE	UpdateTimestamp > @DateFrom
			AND UpdateTimestamp <= @DateTo
	OPTION (RECOMPILE)

	EXEC Log.Logging	@DateLog,@Me,''Changes'',''Start'',@RowsProcessed=@@ROWCOUNT

	SELECT	a.*, CASE WHEN b.UpdateTimestamp IS NULL THEN ''I'' ELSE ''U'' END Upsert
	INTO	#t
	FROM	'+@TableFrom+' a 
			LEFT JOIN '+@TableTo+' b ON '+@KeyJoin+'
	WHERE	a.UpdateTimestamp > @DateFrom
			AND a.UpdateTimestamp <= @DateTo
			AND a.'+@CandidateKey+' IN (SELECT '+@CandidateKey+' FROM #x)
	OPTION (RECOMPILE)

	EXEC Log.Logging	@DateLog,@Me,''Update'',''Start'',@RowsProcessed=@@ROWCOUNT

	UPDATE a
	SET	'+@Update+'
	FROM '+@TableTo+' a INNER JOIN #t b ON '+@KeyJoin+'
	WHERE b.Upsert = ''U''

	EXEC Log.Logging	@DateLog,@Me,''Insert'',''Start'',@RowsProcessed=@@ROWCOUNT

	INSERT '+@TableTo+'('+REPLACE(@Columns,'a.','')+')
	SELECT '+@Columns+'
	FROM	#t a
	WHERE	Upsert = ''I''

	EXEC Log.Logging	@DateLog,@Me,NULL,''Success'',@RowsProcessed=@@ROWCOUNT
	'

	DECLARE @Me varchar(255) = @TableTo
	DECLARE @Message varchar(1000)
	DECLARE @RowCount int

	DECLARE @DateWatermark datetime2(7)

BEGIN TRY

	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED

	DECLARE	@SQLDateWatermark nvarchar(MAX) = 'SELECT @DateWatermark = SYSUTCDATETIME()'
	EXEC dbo.sp_executesql @SQLDateWatermark, N'@DateWatermark datetime2(7) OUTPUT', @DateWatermark=@DateWatermark OUTPUT
	IF @DateTo < @DateWatermark SET @DateWatermark = @DateTo -- If a supplied @DateTo is less than the permitted watermark, make it the new watermark

	EXEC Log.Logging	@DateWatermark,@Me,'Dates','Start'

	DECLARE	@SQLDateFrom nvarchar(MAX) = 'SELECT @DateFrom = ISNULL(MAX(UpdateTimestamp),''1899-01-01'') FROM '+@TableTo
	IF @DateFrom IS NULL EXEC dbo.sp_executesql @SQLDateFrom, N'@DateFrom datetime2(7) OUTPUT', @DateFrom=@DateFrom OUTPUT

	DECLARE	@SQLDateTo nvarchar(MAX) = 'SELECT @DateTo = ISNULL(MAX(UpdateTimestamp), ''1899-01-01'') FROM '+@TableFrom+' WHERE UpdateTimestamp >= CONVERT(datetime2,'''+CONVERT(varchar(40),@DateFrom)+''') AND UpdateTimestamp <= CONVERT(datetime2,'''+CONVERT(varchar(40),@DateWatermark)+''')' 
	EXEC dbo.sp_executesql @SQLDateTo, N'@DateTo datetime2(7) OUTPUT', @DateTo=@Dateto OUTPUT
	--EXEC dbo.sp_executesql @SQLDateTo, N'@DateFrom datetime2(7), @DateWatermark datetime2(7), @DateTo datetime2(7) OUTPUT', @DateFrom=@DateFrom, @DateWatermark=@DateWatermark, @DateTo=@Dateto OUTPUT

	SET @Message  = CONVERT(varchar,@DateFrom,121)+' - '+CONVERT(varchar,@DateTo,121)

	IF @DateTo > @DateFrom SET @RowCount = 1 ELSE SET @RowCount = 0

	EXEC Log.Logging	@DateWatermark,@Me,NULL,'Success', @Message,@RowsProcessed=@RowCount

	IF @RowCount = 0 RETURN -- Nothing to do?

	EXEC Log.Logging	@DateWatermark,@Me,'Code','Start',@SQL

	IF @DryRun = 0 EXEC dbo.sp_executesql @SQL, N'@DateFrom datetime2(7), @DateTo datetime2(7), @DateLog datetime2(7), @Me varchar(255)', @DateFrom=@DateFrom, @DateTo=@DateTo, @DateLog=@DateWatermark, @Me=@Me

	EXEC Log.Logging	@DateWatermark,@Me,NULL,'Success'

END TRY
BEGIN CATCH
	DECLARE @ErrorMessage VARCHAR(1000) = ERROR_MESSAGE();

	EXEC Log.Logging	@DateWatermark, @Me, NULL, 'Failed', @ErrorMessage;

	THROW;

END CATCH
GO


