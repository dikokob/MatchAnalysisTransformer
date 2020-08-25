SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[spRemoveExistingSecondPhaseSetPieces] @SecondPhaseSetPiecesData [dbo].[SecondPhaseSetPiecesType] READONLY
AS
BEGIN
	DECLARE @game_id as nvarchar(50);
	DECLARE @Fixture as NVARCHAR(50);

	DECLARE @Cursor as CURSOR;

	SET @Cursor = CURSOR FOR
	SELECT DISTINCT [game_id], [Fixture] FROM @SecondPhaseSetPiecesData;

	OPEN @Cursor;
	FETCH NEXT FROM @Cursor INTO @game_id, @Fixture;

	WHILE @@FETCH_STATUS = 0
	BEGIN
	 PRINT 'Deleting SecondPhaseSetPieces for game_id: ' + @game_id + ', Fixture: ' + @Fixture;
	 DELETE FROM [dbo].[SecondPhaseSetPieces] where Id IN (SELECT Id FROM [dbo].[SecondPhaseSetPieces] where [game_id] = @game_id AND [Fixture] = @Fixture)
	 FETCH NEXT FROM @Cursor INTO @game_id, @Fixture;
	END

	CLOSE @Cursor;
	DEALLOCATE @Cursor;

    INSERT [dbo].[SecondPhaseSetPieces](
	[game_id],
	[Fixture],
	[Attacking Team],
	[Defending Team],
	[Attacking Team ID],
	[Defending Team ID],
	[Goals Scored], 
	[Goals Conceded],
	[Goal Difference],
	[Game State],
	[Side],
	[Number Events In Window],
	[Direct],
	[OPTA Event ID],
	[period_id],
	[min],
	[sec],
	[X Coordinate],
	[Y Coordinate],
	[End X Coordinate],
	[End Y Coordinate],
	[Player ID],
	[Player Name],
	[% Distance Along X],
	[Length Pass],
	[Relevant OPTA Event ID],
	[Relevant min],
	[Relevant sec],
	[Relevant X Coordinate],
	[Relevant Y Coordinate],
	[Relevant End X Coordinate],
	[Relevant End Y Coordinate],
	[Relevant Player ID],
	[Relevant Player Name],
	[Relevant % Distance Along X],
	[Relevant Length Pass],
	[Start Area Of Pitch],
	[Freekick Starts After Box],
	[Frontal/Lateral Start],
	[Frontal/Lateral End],
	[Ending Side],
	[Time Lapsed From Stop and Start],
	[Number Of Events Between Stop and Start],
	[OPTA Event IDs Between Stop and Start],
	[Player IDs In Pass Sequence Up To Relevant],
	[Player Name In Pass Sequence Up To Relevant],
	[Rolled],
	[First Contact Type],
	[First Contact Explanation],
	[First Contact Player ID],
	[First Contact Player Name],
	[First Contact Team ID],
	[First Contact Team Name],
	[First Contact Aerial],
	[Defending Goalkeeper ID],
	[Defending Goalkeeper Name],
	[Set Piece Type],
	[Starting Delivery Type],
	[Actual Delivery Type],
	[Passed To Edge of Box],	
	[Passed In Behind],
	[Preferred Foot],
	[Relevant Preferred Foot],
	[Time_in_Seconds],
	[Time_in_Seconds_Relavant],
	[2nd Phase Cross OPTA Event ID]
	)
    SELECT * FROM @SecondPhaseSetPiecesData
END
GO


