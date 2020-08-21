SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[spRemoveExistingSecondPhaseShotsfromSetPieces] @SecondPhaseShotsfromSetPiecesData [dbo].[SecondPhaseShotsfromSetPiecesType] READONLY
AS
BEGIN
	DECLARE @GameID as nvarchar(500);
	DECLARE @CompetitionID as NVARCHAR(100);

	DECLARE @Cursor as CURSOR;

	SET @Cursor = CURSOR FOR
	SELECT DISTINCT [Game ID], [Competition ID] FROM @SecondPhaseShotsfromSetPiecesData;

	OPEN @Cursor;
	FETCH NEXT FROM @Cursor INTO @GameID, @CompetitionID;

	WHILE @@FETCH_STATUS = 0
	BEGIN
	 PRINT 'Deleting SecondPhaseShotsfromSetPieces for GameID: ' + @GameID + ', CompetitonID: ' + @CompetitionID;
	 DELETE FROM [dbo].[SecondPhaseShotsfromSetPieces] where Id IN (SELECT Id FROM [dbo].[SecondPhaseShotsfromSetPieces] where [Game ID] = @GameID AND [Competition ID] = @CompetitionID)
	 FETCH NEXT FROM @Cursor INTO @GameID, @CompetitionID;
	END

	CLOSE @Cursor;
	DEALLOCATE @Cursor;

    INSERT [dbo].[SecondPhaseShotsfromSetPieces](
	[Set Piece OPTA Event ID],



	)
    SELECT * FROM @SecondPhaseShotsfromSetPiecesData
END
GO


