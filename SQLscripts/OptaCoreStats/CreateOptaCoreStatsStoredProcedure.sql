SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO




CREATE PROCEDURE [dbo].[spRemoveExistingOptaCoreStats] @OptaCoreStatsData [dbo].[OptaCoreStatsType] READONLY
AS
BEGIN
	DECLARE @GameID as nvarchar(500);
	DECLARE @CompetitionID as NVARCHAR(100);

	DECLARE @Cursor as CURSOR;

	SET @Cursor = CURSOR FOR
	SELECT DISTINCT [Game ID], [Competition ID] FROM @OptaCoreStatsData;

	OPEN @Cursor;
	FETCH NEXT FROM @Cursor INTO @GameID, @CompetitionID;

	WHILE @@FETCH_STATUS = 0
	BEGIN
	 PRINT 'Deleting OptaCoreStats for GameID: ' + @GameID + ', CompetitonID: ' + @CompetitionID;
	 DELETE FROM [dbo].[OptaCoreStats] where Id IN (SELECT Id FROM [dbo].[OptaCoreStats] where [Game ID] = @GameID AND [Competition ID] = @CompetitionID)
	 FETCH NEXT FROM @Cursor INTO @GameID, @CompetitionID;
	END

	CLOSE @Cursor;
	DEALLOCATE @Cursor;

    INSERT [dbo].[OptaCoreStats](
	[Competition ID],
	[Competition Name],
	[Date],
	[Game ID],
	[Home/Away],
	[Opposition Team Formation],
	[Opposition Team Formation ID],
	[Opposition Team ID],
	[Opposition Team Name],
	[Player ID],
	[Player Name],
	[Position ID] ,
	[Season ID],
	[Season Name],
	[Start],
	[Substitute Off],
	[Substitute On],
	[Team Formation],
	[Team Formation ID],
	[Team ID],
	[Team Name],
    [Sent Off],
    [Retired],
	[Time Played],
	[Time In Possession],
	[Time Out Of Possession],
    [Time Played Calculated From Tracking Data]
	)
    SELECT * FROM @OptaCoreStatsData
END
GO
