SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO



CREATE PROCEDURE [dbo].[spRemoveExistingCrosses] @CrossesData [dbo].[CrossesType] READONLY
AS
BEGIN
	DECLARE @game_id as nvarchar(500);
	DECLARE @Fixture as NVARCHAR(100);

	DECLARE @Cursor as CURSOR;

	SET @Cursor = CURSOR FOR
	SELECT DISTINCT [game_id], [Fixture] FROM @CrossesData;

	OPEN @Cursor;
	FETCH NEXT FROM @Cursor INTO @game_id, @Fixture;

	WHILE @@FETCH_STATUS = 0
	BEGIN
	 PRINT 'Deleting Crosses for GameID: ' + @game_id + ', Fixture: ' + @Fixture;
	 DELETE FROM [dbo].[Crosses] where Id IN (SELECT Id FROM [dbo].[Crosses] where [game_id] = @game_id AND [Fixture] = @Fixture)
	 FETCH NEXT FROM @Cursor INTO @game_id, @Fixture;
	END

	CLOSE @Cursor;
	DEALLOCATE @Cursor;

    INSERT [dbo].[Crosses](
	[game_id],
	[Fixture],
	[Attacking Team],
	[Defending Team],
	[Attacking Team ID],
	[Defending Team ID],
	[Goals Scored],
	[Goals Conceded],
	[Goals Difference],
	[Game State],
	[Side],
	[Early/Lateral/Deep],
	[OPTA Event ID],
	[period_id],
	[min],
	[sec],
	[X Coordinate],
	[Y Coordinate],
	[End X Coordinate],
	[End Y Coordinate],
	[Length Pass],
	[% Distance Along X],
	[Player ID],
	[Player Name],
	[Preferred Foot],
	[Outcome],
	[Keypass/Assist],
	[Blocked Pass],
	[Cutback],
	[OPTA Pull Back Qualifier],
	[Out Of Pitch],
	[Ending Too Wide],
	[Cross Type],
	[Set Piece OPTA Event ID],
	[OPTA Cross Qualifier],
	[Time Between Set Piece and Cross],
	[Number Events Between Set Piece and Cross],
	[Linked 2nd Phase Cross IDs],
	[First Contact Type],
	[First Contact Explanation],
	[First Contact Player ID],
	[First Contact Player Name],
	[First Contact Team ID],
	[First Contact Team Name],
	[First Contact Aerial],
	[Defending Goalkeeper ID],
	[Defending Goalkeeper Name]
	)
    SELECT * FROM @CrossesData
END
GO


