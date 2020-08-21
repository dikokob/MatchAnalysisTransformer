SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[spRemoveExistingCrosses] @CrossesData [dbo].[CrossesType] READONLY
AS
BEGIN
	DECLARE @GameID as nvarchar(500);
	DECLARE @CompetitionID as NVARCHAR(100);

	DECLARE @Cursor as CURSOR;

	SET @Cursor = CURSOR FOR
	SELECT DISTINCT [Game ID], [Competition ID] FROM @CrossesData;

	OPEN @Cursor;
	FETCH NEXT FROM @Cursor INTO @GameID, @CompetitionID;

	WHILE @@FETCH_STATUS = 0
	BEGIN
	 PRINT 'Deleting Crosses for GameID: ' + @GameID + ', CompetitonID: ' + @CompetitionID;
	 DELETE FROM [dbo].[Crosses] where Id IN (SELECT Id FROM [dbo].[Crosses] where [Game ID] = @GameID AND [Competition ID] = @CompetitionID)
	 FETCH NEXT FROM @Cursor INTO @GameID, @CompetitionID;
	END

	CLOSE @Cursor;
	DEALLOCATE @Cursor;

    INSERT [dbo].[Crosses](
	[game_id],



	)
    SELECT * FROM @CrossesData
END
GO


