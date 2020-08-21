SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[spRemoveExistingAerialDuelsfromCrosses] @AerialDuelsfromCrossesData [dbo].[AerialDuelsfromCrossesType] READONLY
AS
BEGIN
	DECLARE @GameID as nvarchar(500);
	DECLARE @CompetitionID as NVARCHAR(100);

	DECLARE @Cursor as CURSOR;

	SET @Cursor = CURSOR FOR
	SELECT DISTINCT [Game ID], [Competition ID] FROM @AerialDuelsfromCrossesData;

	OPEN @Cursor;
	FETCH NEXT FROM @Cursor INTO @GameID, @CompetitionID;

	WHILE @@FETCH_STATUS = 0
	BEGIN
	 PRINT 'Deleting AerialDuelsfromCrosses for GameID: ' + @GameID + ', CompetitonID: ' + @CompetitionID;
	 DELETE FROM [dbo].[AerialDuelsfromCrosses] where Id IN (SELECT Id FROM [dbo].[AerialDuelsfromCrosses] where [Game ID] = @GameID AND [Competition ID] = @CompetitionID)
	 FETCH NEXT FROM @Cursor INTO @GameID, @CompetitionID;
	END

	CLOSE @Cursor;
	DEALLOCATE @Cursor;

    INSERT [dbo].[AerialDuelsfromCrosses](
	[Cross OPTA Event ID],



	)
    SELECT * FROM @AerialDuelsfromCrossesData
END
GO


