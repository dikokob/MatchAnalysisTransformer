SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

Drop PROCEDURE [dbo].[spRemoveExistingAerialDuelsfromCrosses] 
Go


CREATE PROCEDURE [dbo].[spRemoveExistingAerialDuelsfromCrosses] @AerialDuelsfromCrossesData [dbo].[AerialDuelsfromCrossesType] READONLY
AS
BEGIN
	DECLARE @game_id as nvarchar(500);
	DECLARE @Fixture as NVARCHAR(100);

	DECLARE @Cursor as CURSOR;

	SET @Cursor = CURSOR FOR
	SELECT DISTINCT [game_id], [Fixture] FROM @AerialDuelsfromCrossesData;

	OPEN @Cursor;
	FETCH NEXT FROM @Cursor INTO @game_id, @Fixture;

	WHILE @@FETCH_STATUS = 0
	BEGIN
	 PRINT 'Deleting AerialDuelsfromCrosses for game_id: ' + @game_id + ', CompetitonID: ' + @Fixture;
	 DELETE FROM [dbo].[AerialDuelsfromCrosses] where Id IN (SELECT Id FROM [dbo].[AerialDuelsfromCrosses] where [game_id] = @game_id AND [Fixture] = @Fixture)
	 FETCH NEXT FROM @Cursor INTO @game_id, @Fixture;
	END

	CLOSE @Cursor;
	DEALLOCATE @Cursor;

    INSERT [dbo].[AerialDuelsfromCrosses](
	[game_id],
	[Fixture],
	[Cross OPTA Event ID],
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
	[Other Aerial Duel Is Shot],
	[Other X Coordinate Player],
	[Other Y Coordinate Player],
	[X Coordinate Player],
	[Y Coordinate Player]

	)
    SELECT * FROM @AerialDuelsfromCrossesData
END
GO


