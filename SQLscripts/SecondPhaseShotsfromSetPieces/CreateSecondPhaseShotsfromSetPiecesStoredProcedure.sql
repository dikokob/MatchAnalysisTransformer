SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[spRemoveExistingSecondPhaseShotsfromSetPieces] @SecondPhaseShotsfromSetPiecesData [dbo].[SecondPhaseShotsfromSetPiecesType] READONLY
AS
BEGIN
	DECLARE @game_id as nvarchar(50);
	DECLARE @Fixture as NVARCHAR(50);

	DECLARE @Cursor as CURSOR;

	SET @Cursor = CURSOR FOR
	SELECT DISTINCT [game_id], [Fixture] FROM @SecondPhaseShotsfromSetPiecesData;

	OPEN @Cursor;
	FETCH NEXT FROM @Cursor INTO @game_id, @Fixture;

	WHILE @@FETCH_STATUS = 0
	BEGIN
	 PRINT 'Deleting SecondPhaseShotsfromSetPieces for game_id: ' + @game_id + ', CompetitonID: ' + @Fixture;
	 DELETE FROM [dbo].[SecondPhaseShotsfromSetPieces] where Id IN (SELECT Id FROM [dbo].[SecondPhaseShotsfromSetPieces] where [game_id] = @game_id AND [Fixture] = @Fixture)
	 FETCH NEXT FROM @Cursor INTO @game_id, @Fixture;
	END

	CLOSE @Cursor;
	DEALLOCATE @Cursor;

    INSERT [dbo].[SecondPhaseShotsfromSetPieces](
	[game_id],
	[Fixture],
	[Set Piece OPTA Event ID],
	[Shot OPTA ID],
	[Shot Player ID],
	[Shot Player Name],
	[Shot Team ID],
	[Shot Team Name],
	[Shot Occurrence],
	[Shot Outcome],
	[Shot Body Part],
	[Aerial Duel Is Shot],
	[Preferred Foot],
	[2nd Phase Cross],
	[2nd Phase Cross OPTA Event ID]

	)
    SELECT * FROM @SecondPhaseShotsfromSetPiecesData
END
GO


