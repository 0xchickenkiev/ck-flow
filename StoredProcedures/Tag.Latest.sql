USE [WH_Control]
GO

/****** Object:  StoredProcedure [Tag].[Latest]    Script Date: 06/02/2023 15:06:43 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Tag].[Latest]
(
	@TableFrom	varchar(max) = NULL,
	@TableTo	varchar(max) = NULL,
	@DateFrom	datetime2(7) = NULL OUTPUT,
	@DateTo		datetime2(7) = NULL OUTPUT,
	@DryRun		bit = 0
)
AS

	SET NOCOUNT ON

	DECLARE	@KeyColumns varchar(max) = STUFF((SELECT ', a.'+QUOTENAME(name) FROM sys.dm_exec_describe_first_result_set('SELECT * FROM '+@TableTo,NULL,1) WHERE is_part_of_unique_key = 1 ORDER BY column_ordinal FOR XML PATH('')),1,2,'')
	DECLARE	@KeyJoin varchar(max) = STUFF((SELECT ' AND b.'+QUOTENAME(name)+' = a.'+QUOTENAME(name) FROM sys.dm_exec_describe_first_result_set('SELECT * FROM '+@TableTo,NULL,1) WHERE is_part_of_unique_key = 1 ORDER BY column_ordinal FOR XML PATH('')),1,5,'')

	--DECLARE	@SQLDateWatermark nvarchar(MAX) = 'SELECT @DateWatermark = ISNULL(LakeTimestamp, ''1900-01-01'') FROM '+SUBSTRING(@TableTo, 1, LEN(@TableTo)-CHARINDEX('.',REVERSE(@TableTo)+'.'))+'.__$HighWatermark'
	--DECLARE	@SQLDateFrom nvarchar(MAX) = 'SELECT @DateFrom = ISNULL(MAX(LakeTimestamp),''1900-01-01'') FROM '+@TableTo+' OPTION (RECOMPILE)'
	--DECLARE	@SQLDateTo nvarchar(MAX) = 'SELECT @DateTo = ISNULL(MAX(LakeTimestamp), ''1900-01-01'') FROM '+@TableFrom+' WHERE LakeTimestamp >= @DateFrom AND LakeTimestamp <= @DateWatermark OPTION (RECOMPILE)' 
	DECLARE @SQL nvarchar(MAX) = 
	'EXEC Log.Logging	@DateLog,@Me,''Changes'',''Start'',@RowsProcessed=0
	
	SELECT	'+@KeyColumns+', CaptureTimestamp, LakeTimestamp, LakeActionCode, Upsert
	INTO #t
	FROM	(	SELECT	'+@KeyColumns+', a.CaptureTimestamp, a.LakeTimestamp, a.LakeActionCode, CASE WHEN b.LakeActionCode IS NULL THEN ''I'' ELSE ''U'' END Upsert, ROW_NUMBER() OVER (PARTITION BY '+@KeyColumns+' ORDER BY a.CaptureTimestamp DESC, a.LakeTimestamp DESC, a.LakeActionCode DESC) r -- Note the use of Lake action code as final test is for the scenario where we have a delete and reinsert, to ensure the reinsert is rownumber 1 after taking everything else into account
				FROM	'+@TableFrom+' a 
						LEFT JOIN '+@TableTo+' b ON '+@KeyJoin+'
				WHERE	a.LakeTimestamp > @DateFrom
						AND a.LakeTimestamp <= @DateTo
						AND ISNULL(b.CaptureTimestamp,''1900-01-01'') <= a.CaptureTimestamp -- not strictly necessary for CDC sources, but here to cope if we ever get data loaded into lake out of source date order
			) a
	WHERE	r=1
	OPTION (RECOMPILE)

	EXEC Log.Logging	@DateLog,@Me,''Update'',''Start'',@RowsProcessed=@@ROWCOUNT

	UPDATE a
	SET CaptureTimestamp = b.CaptureTimestamp,
		LakeTimestamp = b.LakeTimestamp,
		LakeActionCode = b.LakeActionCode,
		UpdateTimestamp = SYSUTCDATETIME()
	FROM '+@TableTo+' a INNER JOIN #t b ON '+@KeyJoin+'
	WHERE b.Upsert = ''U''

	EXEC Log.Logging	@DateLog,@Me,''Insert'',''Start'',@RowsProcessed=@@ROWCOUNT

	INSERT '+@TableTo+'('+REPLACE(@KeyColumns,'a.','')+', CaptureTimestamp, LakeTimestamp, LakeActionCode, UpdateTimestamp)
	SELECT '+@KeyColumns+', a.CaptureTimestamp, a.LakeTimestamp, a.LakeActionCode, SYSUTCDATETIME() UpdateTimestamp
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


