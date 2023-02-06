USE [WH_Control]
GO

/****** Object:  StoredProcedure [Log].[Logging]    Script Date: 06/02/2023 15:06:01 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [Log].[Logging]
(
	@LoadDate		DATETIME2(7),			-- Load date up to which this ETL will run
	@Procedure		VARCHAR(128),			-- Name of the SP
	@Step			VARCHAR(128) = NULL,	-- Not Mandatory, can be for a Step or the whole Proc
	@Status			VARCHAR(128),			-- Start,Success,Failed
	@Message		VARCHAR(MAX) = NULL,	-- Message passed such as context information or error message for failure
	@RowsProcessed	BIGINT = NULL
)
AS 
BEGIN

		DECLARE @LogId		INT;
		DECLARE @Run		SMALLINT;
		DECLARE @SQL nvarchar(MAX);

		SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;

		IF @@TRANCOUNT > 0
			BEGIN -- There is a current active transaction, so recall this procedure via the autonomous recursive link so that the log entry can be committed independent of the current active transaction
				SELECT	@SQL = '[LOOPBACK_AUTONOMOUS].['+db_name()+'].['+OBJECT_SCHEMA_NAME(@@PROCID)+'].['+OBJECT_NAME(@@PROCID)+'] @BatchId, @Procedure, @Step, @Status, @Message, @RowsProcessed';

				--SELECT @SQL;
				EXEC sp_executesql @SQL, N'@LoadDate DATETIME2(7), @Procedure VARCHAR(128), @Step VARCHAR(128), @Status VARCHAR(128), @Message VARCHAR(1000), @RowsProcessed BIGINT', 
								   @LoadDate = @LoadDate, @Procedure = @Procedure, @Step = @Step, @Status = @Status, @Message = @Message, @RowsProcessed = @RowsProcessed;
			END
		ELSE
		BEGIN

			-- Get the latest run number for nominated Procedure and LoadDate
			SELECT @Run = ISNULL(MAX(Run),1) FROM [Log].[Log] WHERE LoadDate = @LoadDate AND [Procedure] = @Procedure
			-- If the specified Status is 'Start' and there is already a log record for the current step for the latest run number above then add one on to it to start a new run
			IF @Status = 'Start' AND @RowsProcessed IS NULL AND EXISTS (SELECT 1 FROM [Log].[Log] WHERE LoadDate = @LoadDate AND [Procedure] = @Procedure AND Step = @Step AND Run = @Run) SET @Run +=1
			-- Get the current highest LogId for nominated Procedure and LoadDate (should be either current step where this invocation is updating status or previous step where starting a new stap)
			SELECT @LogId = MAX(LogId) FROM [Log].[Log] WHERE LoadDate = @LoadDate AND [Procedure] = @Procedure AND Run = @Run;

			IF @Status='Start'
			BEGIN

				-- Close off (assumed) previous step as successful based on start of new step within same procedure (this does have undesirable side effect of marking previous incomplete step successful if procedure is abandoned and restarted, but avoids need to explitily end each successful step before starting the next)
				UPDATE [Log].[Log]
				SET Status		=	'Success',
					EndDateTime	=	ISNULL(EndDateTime,GETDATE()),
					RowsProcessed	=	ISNULL(RowsProcessed,@RowsProcessed)
				WHERE LogId = @LogId
					AND LoadDate = @LoadDate -- included for performance in case we partition by LoadDate at some point in future
					AND Status = 'Running';

				INSERT INTO [Log].[Log] (LoadDate, Run,[Procedure],Step,[Status],StartDateTime,EndDateTime,Message)	
				SELECT @LoadDate, @Run, @Procedure, @Step, 'Running', GETDATE(), NULL, @Message

			END

			IF @Status='Success'
			BEGIN

				UPDATE [Log].[Log]
				SET Status			=	'Success',
					EndDateTime		=	GETDATE(),
					Message			=	ISNULL(@Message,Message),
					RowsProcessed	=	ISNULL(@RowsProcessed,RowsProcessed)
				WHERE LogId = @LogId
					AND LoadDate = @LoadDate -- included for performance in case we partition by LoadDate at some point in future
			END

			IF @Status='Failed'
			BEGIN

				UPDATE [Log].[Log]
				SET Status			=	'Failed',
					EndDateTime		=	GETDATE(),
					Message			=	@Message,
					RowsProcessed	=	@RowsProcessed
				WHERE LogId=@LogId
					AND LoadDate = @LoadDate -- included for performance in case we partition by LoadDate at some point in future
			END

		END
END
GO


