SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[spRemoveExistingAerialDuelsSetPieces] @AerialDuelsSetPiecesData [dbo].[AerialDuelsSetPiecesType] READONLY
AS
BEGIN
	DECLARE @GameID as nvarchar(500);
	DECLARE @CompetitionID as NVARCHAR(100);

	DECLARE @Cursor as CURSOR;

	SET @Cursor = CURSOR FOR
	SELECT DISTINCT [Game ID], [Competition ID] FROM @AerialDuelsSetPiecesData;

	OPEN @Cursor;
	FETCH NEXT FROM @Cursor INTO @GameID, @CompetitionID;

	WHILE @@FETCH_STATUS = 0
	BEGIN
	 PRINT 'Deleting AerialDuelsSetPieces for GameID: ' + @GameID + ', CompetitonID: ' + @CompetitionID;
	 DELETE FROM [dbo].[AerialDuelsSetPieces] where Id IN (SELECT Id FROM [dbo].[AerialDuelsSetPieces] where [Game ID] = @GameID AND [Competition ID] = @CompetitionID)
	 FETCH NEXT FROM @Cursor INTO @GameID, @CompetitionID;
	END

	CLOSE @Cursor;
	DEALLOCATE @Cursor;

    INSERT [dbo].[AerialDuelsSetPieces](
	[Set Piece OPTA Event ID],
	[Aerial Duel OPTA ID],


	)
    SELECT * FROM @AerialDuelsSetPiecesData
END
GO


