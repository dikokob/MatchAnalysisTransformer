SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[spRemoveExistingAerialDuelsSetPieces] @AerialDuelsSetPiecesData [dbo].[AerialDuelsSetPiecesType] READONLY
AS
BEGIN
	DECLARE @game_id as nvarchar(50);
	DECLARE @Fixture as NVARCHAR(50);

	DECLARE @Cursor as CURSOR;

	SET @Cursor = CURSOR FOR
	SELECT DISTINCT [game_id], [Fixture] FROM @AerialDuelsSetPiecesData;

	OPEN @Cursor;
	FETCH NEXT FROM @Cursor INTO @game_id, @Fixture;

	WHILE @@FETCH_STATUS = 0
	BEGIN
	 PRINT 'Deleting AerialDuelsSetPieces for game_id: ' + @game_id + ', Fixture: ' + @Fixture;
	 DELETE FROM [dbo].[AerialDuelsSetPieces] where Id IN (SELECT Id FROM [dbo].[AerialDuelsSetPieces] where [game_id] = @game_id AND [Fixture] = @Fixture)
	 FETCH NEXT FROM @Cursor INTO @game_id, @Fixture;
	END

	CLOSE @Cursor;
	DEALLOCATE @Cursor;

    INSERT [dbo].[AerialDuelsSetPieces](
    [game_id],
    [Fixture],
	[Set Piece OPTA Event ID],
    [Aerial Duel OPTA ID],
	[Aerial Duel Player ID],
	[Aerial Duel Player Name],
	[Aerial Duel Team ID],
	[Aerial Duel Team Name],
    [Successful/Unsuccessful],
	[Other Aerial Duel Player ID],
	[Other Aerial Duel Player Name],
	[Other Aerial Duel Team ID],
	[Other Aerial Duel Team Name],
	[Aerial Duel Is Shot]
	)
    SELECT * FROM @AerialDuelsSetPiecesData
END
GO


