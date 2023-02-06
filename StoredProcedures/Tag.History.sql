USE [WH_Control]
GO

/****** Object:  StoredProcedure [Tag].[History]    Script Date: 06/02/2023 15:06:32 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Tag].[History]
(
	@TableFrom	varchar(max) = NULL,
	@TableTo	varchar(max) = NULL,
	@DateFrom	datetime2(7) = NULL OUTPUT,
	@DateTo		datetime2(7) = NULL OUTPUT,
	@DryRun		bit = 0
)
AS

	SET NOCOUNT ON

	DECLARE	@KeyColumns varchar(max) = STUFF((SELECT ', a.'+QUOTENAME(name) FROM sys.dm_exec_describe_first_result_set('SELECT * FROM '+@TableTo,NULL,1) WHERE is_part_of_unique_key = 1 AND name <> 'LakeTimestamp' ORDER BY column_ordinal FOR XML PATH('')),1,2,'')
	DECLARE	@KeyJoin varchar(max) = STUFF((SELECT ' AND b.'+QUOTENAME(name)+' = a.'+QUOTENAME(name) FROM sys.dm_exec_describe_first_result_set('SELECT * FROM '+@TableTo,NULL,1) WHERE is_part_of_unique_key = 1 AND name <> 'LakeTimestamp' ORDER BY column_ordinal FOR XML PATH('')),1,5,'')

	--DECLARE	@SQLDateWatermark nvarchar(MAX) = 'SELECT @DateWatermark = ISNULL(LakeTimestamp, ''1900-01-01'') FROM '+SUBSTRING(@TableTo, 1, LEN(@TableTo)-CHARINDEX('.',REVERSE(@TableTo)+'.'))+'.__$HighWatermark'
	--DECLARE	@SQLDateFrom nvarchar(MAX) = 'SELECT @DateFrom = ISNULL(MAX(LakeTimestamp),''1900-01-01'') FROM '+@TableTo+' OPTION (RECOMPILE)'
	--DECLARE	@SQLDateTo nvarchar(MAX) = 'SELECT @DateTo = ISNULL(MAX(LakeTimestamp), ''1900-01-01'') FROM '+@TableFrom+' WHERE LakeTimestamp >= @DateFrom AND LakeTimestamp <= @DateWatermark OPTION (RECOMPILE)' 
	DECLARE @SQL nvarchar(MAX) = 
	'EXEC Log.Logging	@DateLog,@Me,''Changes'',''Start'',@RowsProcessed=0
	
	SELECT	'+@KeyColumns+', a.CaptureTimestamp, a.LakeTimestamp, a.LakeActionCode, ''I'' Upsert
	INTO	#s
	FROM	'+@TableFrom+' a
	WHERE	a.LakeTimestamp > @DateFrom
			AND a.LakeTimestamp <= @DateTo
	OPTION (RECOMPILE)

	EXEC Log.Logging	@DateLog,@Me,''Existing'',''Start'',@RowsProcessed=@@ROWCOUNT

	INSERT	#s
	SELECT	'+@KeyColumns+', a.CaptureTimestamp, a.LakeTimestamp, a.LakeActionCode, ''U'' Upsert
	FROM	'+@TableTo+' a
			INNER JOIN (SELECT '+@KeyColumns+', MIN(a.CaptureTimestamp) CaptureTimestamp FROM #s a GROUP BY '+@KeyColumns+') b ON '+@KeyJoin+' AND b.CaptureTimestamp < a.ExpiryTimestamp
	OPTION (RECOMPILE)

	EXEC Log.Logging	@DateLog,@Me,''Combine'',''Start'',@RowsProcessed=@@ROWCOUNT

	SELECT	*
	INTO	#t
	FROM	(	SELECT	'+@KeyColumns+', a.CaptureTimestamp, a.LakeTimestamp, a.LakeActionCode,
						MAX(a.Upsert) OVER (PARTITION BY '+@KeyColumns+', a.CaptureTimestamp) Upsert, 
						LEAD(a.CaptureTimestamp, 1, ''9999-12-31'') OVER (PARTITION BY '+@KeyColumns+' ORDER BY a.CaptureTimestamp ASC, a.LakeTimestamp ASC) ExpiryTimestamp
				FROM	#s a
			) x
	WHERE	CaptureTimestamp <> ExpiryTimestamp

	EXEC Log.Logging	@DateLog,@Me,''Update'',''Start'',@RowsProcessed=@@ROWCOUNT

	UPDATE a
	SET ExpiryTimestamp = b.ExpiryTimestamp,
		LakeTimestamp = b.LakeTimestamp,
		UpdateTimestamp = CASE WHEN b.LakeTimestamp = a.LakeTimestamp THEN a.UpdateTimestamp ELSE SYSUTCDATETIME() END
	FROM '+@TableTo+' a INNER JOIN #t b ON '+@KeyJoin+' AND b.CaptureTimestamp = a.CaptureTimestamp
	WHERE b.Upsert = ''U''

	EXEC Log.Logging	@DateLog,@Me,''Insert'',''Start'',@RowsProcessed=@@ROWCOUNT

	INSERT '+@TableTo+'('+REPLACE(@KeyColumns,'a.','')+', CaptureTimestamp, ExpiryTimestamp, LakeTimestamp, LakeActionCode, UpdateTimestamp)
	SELECT '+@KeyColumns+', a.CaptureTimestamp, a.ExpiryTimestamp, a.LakeTimestamp, a.LakeActionCode, SYSUTCDATETIME() UpdateTimestamp
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

	DECLARE	@SQLDateWatermark nvarchar(MAX) = 'SELECT @DateWatermark = ISNULL(LakeTimestamp, ''1900-01-01'') FROM '+SUBSTRING(@TableTo, 1, LEN(@TableTo)-CHARINDEX('.',REVERSE(@TableTo)+'.'))+'.__$HighWatermark'
	EXEC dbo.sp_executesql @SQLDateWatermark, N'@DateWatermark datetime2(7) OUTPUT', @DateWatermark=@DateWatermark OUTPUT
	IF @DateTo < @DateWatermark SET @DateWatermark = @DateTo -- If a supplied @DateTo is less than the permitted watermark, make it the new watermark

	EXEC Log.Logging	@DateWatermark,@Me,'Dates','Start'

	DECLARE	@SQLDateFrom nvarchar(MAX) = 'SELECT @DateFrom = ISNULL(MAX(LakeTimestamp),''1900-01-01'') FROM '+@TableTo
	IF @DateFrom IS NULL EXEC dbo.sp_executesql @SQLDateFrom, N'@DateFrom datetime2(7) OUTPUT', @DateFrom=@DateFrom OUTPUT

	DECLARE	@SQLDateTo nvarchar(MAX) = 'SELECT @DateTo = ISNULL(MAX(LakeTimestamp), ''1900-01-01'') FROM '+@TableFrom+' WHERE LakeTimestamp >= CONVERT(datetime2,'''+CONVERT(varchar(40),@DateFrom)+''') AND LakeTimestamp <= CONVERT(datetime2,'''+CONVERT(varchar(40),@DateWatermark)+''')' 
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


