USE [WH_Control]
GO

/****** Object:  StoredProcedure [Control].[History]    Script Date: 06/02/2023 15:05:04 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Control].[History]
(
	@DateLimit datetime2(7) = NULL,
	@DryRun bit = 0
)
AS

	SET NOCOUNT ON

	DECLARE @DB sysname = '[WH_Silver]'
	DECLARE @Me sysname = QUOTENAME(DB_NAME())+'.'+QUOTENAME(OBJECT_SCHEMA_NAME(@@PROCID))+'.'+QUOTENAME(OBJECT_NAME(@@PROCID))

	DECLARE @DateFrom datetime2(7) = NULL,
			@DateTo datetime2(7) = NULL

	DECLARE @SQL nvarchar(MAX)
	DECLARE @RowCount int
	DECLARE @Alert nvarchar(4000), @Message nvarchar(4000)

BEGIN TRY

	IF @DateLimit IS NULL SET @DateLimit = SYSUTCDATETIME() -- enforce a consistent high watermark across all tables for data integrity

	SELECT	'EXEC Tag.History @TableFrom='''+@DB+'.'+QUOTENAME(s.name)+'.'+QUOTENAME(o.name)+''', @TableTo='''+@DB+'.'+QUOTENAME(st.name)+'.'+QUOTENAME(ot.name)+''', @DateFrom=@DateFrom OUTPUT, @DateTo=@DateTo OUTPUT, @DryRun=@DryRun' SQL, 
			@DB+'.'+QUOTENAME(st.name)+'.'+QUOTENAME(ot.name) Target,
			CONVERT(varchar(4000),'') Message,
			CONVERT(datetime2(7),NULL) DateFrom,
			CONVERT(datetime2(7),NULL) DateTo,
			ROW_NUMBER() OVER(ORDER BY st.name, ot.name) i
	INTO	#SQL
	FROM	[WH_Silver].sys.schemas s 
			INNER JOIN [WH_Silver].sys.schemas st ON st.name = REPLACE(s.name,'_Derived','')+'_Tag'
			INNER JOIN [WH_Silver].sys.objects o ON o.schema_id = s.schema_id AND o.type IN ('V','U')
			INNER JOIN [WH_Silver].sys.objects ot ON ot.schema_id = st.schema_id AND ot.name = o.name+'_History' AND ot.type = 'U'
	SET @RowCount = @@ROWCOUNT

	DECLARE @i int = 1
	WHILE @i <= @RowCount
	BEGIN
		SET @DateFrom = NULL
		SET @DateTo = @DateLimit
		SET @Message = ''
		SELECT @SQL = SQL FROM #SQL WHERE i = @i

		BEGIN TRY
			exec sp_executesql @SQL, N'@DateFrom datetime2(7) OUTPUT, @DateTo datetime2(7) OUTPUT, @DryRun bit', @DateFrom=@DateFrom OUTPUT, @DateTo=@DateTo OUTPUT, @DryRun=@DryRun 
		END TRY 
		BEGIN CATCH 
			SET @Message = ERROR_MESSAGE()
		END CATCH

		UPDATE #SQL SET Message = @Message, DateFrom = @DateFrom, DateTo = @DateTo WHERE i = @i 

		SET @i += 1
	END

	IF EXISTS (SELECT 1 FROM #SQL WHERE Message <> '')
		BEGIN
			SET @Message = 'The following table(s) failed '+@Me+' processing:'  + CHAR(13) + CHAR(10) + 
								(	SELECT   CHAR(10)+CHAR(9)+Target+' : '+ CHAR(9)+'Error = '+ Message
									FROM #SQL
									WHERE Message <> ''
									ORDER BY i
									FOR XML PATH(''))
			RAISERROR (@Message, 16, 1)
		END

END TRY
BEGIN CATCH 
	IF @@TRANCOUNT > 0 ROLLBACK
			SET @Alert = @@SERVERNAME+' - '+@DB+' - Data ALT ETL Failed'
	SET @Message =  @Me+'  Failed with error message:'+CHAR(13)+CHAR(10)+CHAR(13)+CHAR(10)+ERROR_MESSAGE()
	IF @DryRun = 0
		EXEC [msdb].dbo.sp_send_dbmail
			@profile_name = 'Public',
			@recipients = 'dba@manxtelecom.com',
			@body = @Message,
			@subject = @Alert

	;THROW 
END CATCH
GO


