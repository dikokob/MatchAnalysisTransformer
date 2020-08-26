SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[spRemoveExistingShotsfromCrosses] @ShotsfromCrossesData [dbo].[ShotsfromCrossesType] READONLY
AS
BEGIN
	DECLARE @game_id as nvarchar(500);
	DECLARE @Fixture as NVARCHAR(100);

	DECLARE @Cursor as CURSOR;

	SET @Cursor = CURSOR FOR
	SELECT DISTINCT [game_id], [Fixture] FROM @ShotsfromCrossesData;

	OPEN @Cursor;
	FETCH NEXT FROM @Cursor INTO @game_id, @Fixture;

	WHILE @@FETCH_STATUS = 0
	BEGIN
	 PRINT 'Deleting ShotsfromCrosses for game_id: ' + @game_id + ', CompetitonID: ' + @Fixture;
	 DELETE FROM [dbo].[ShotsfromCrosses] where Id IN (SELECT Id FROM [dbo].[ShotsfromCrosses] where [game_id] = @game_id AND [Fixture] = @Fixture)
	 FETCH NEXT FROM @Cursor INTO @game_id, @Fixture;
	END

	CLOSE @Cursor;
	DEALLOCATE @Cursor;

    INSERT [dbo].[ShotsfromCrosses](
	[game_id],
	[Fixture],
	[Cross OPTA Event ID],
	[Shot OPTA ID],
	[Shot Player ID],
	[Shot Player Name],
	[Shot Team ID],
	[Shot Team Name],
	[Shot Occurrence],
	[Shot Outcome],
	[Shot Body Part],
	[Aerial Duel Is Shot]
	)
    SELECT * FROM @ShotsfromCrossesData
END
GO


