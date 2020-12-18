SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DROP PROCEDURE [dbo].[spRemoveExistingCrosses]
GO

DROP TYPE [dbo].[CrossesType]
GO

CREATE TYPE [dbo].[CrossesType] AS TABLE(
	 [game_id] [nvarchar](50) NULL,
	 [Fixture] [nvarchar](50) NULL,
   	 [Attacking Team] [nvarchar](50) NULL,
	 [Defending Team] [nvarchar](50) NULL,
	 [Attacking Team ID] [nvarchar](50) NULL,
	 [Defending Team ID] [nvarchar](50)	 NULL,
	 [Goals Scored] [int] NULL,
	 [Goals Conceded] [int] NULL,
	 [Goals Difference] [int] NULL,
	 [Game State] [nvarchar](50) NULL,
	 [Side] [nvarchar](50) NULL,
	 [Early/Lateral/Deep] [nvarchar](50) NULL,
	 [OPTA Event ID] [nvarchar](50) NULL,
	 [period_id] [int] NULL,
	 [min] [int] NULL,
	 [sec] [int] NULL,
	 [X Coordinate] [float] NULL,
	 [Y Coordinate] [float] NULL,
	 [End X Coordinate] [float] NULL,
	 [End Y Coordinate] [float] NULL,
	 [Length Pass] [float] NULL,
	 [% Distance Along X] [float] NULL,
	 [Player ID] [nvarchar](50) NULL,
	 [Player Name] [nvarchar](50) NULL,
	 [Preferred Foot] [nvarchar](50) NULL,
	 [Outcome] [nvarchar](50) NULL,
	 [Keypass/Assist] [nvarchar](50) NULL,
	 [Blocked Pass] [bit] NULL,
	 [Cutback] [bit] NULL,
	 [OPTA Pull Back Qualifier] [bit] NULL,
	 [Out Of Pitch] [bit] NULL,
	 [Ending Too Wide] [bit] NULL,
	 [Cross Type] [nvarchar](50) NULL,
	 [Set Piece OPTA Event ID] [nvarchar](50) NULL,
	 [OPTA Cross Qualifier] [bit] NULL,
	 [Time Between Set Piece and Cross] [float] NULL,
	 [Number Events Between Set Piece and Cross] [int] NULL,
	 [Linked 2nd Phase Cross IDs] [nvarchar](50) NULL,
	 [First Contact Type] [int] NULL,
	 [First Contact Explanation] [nvarchar](50) NULL,
	 [First Contact Player ID] [nvarchar](50) NULL,
	 [First Contact Player Name] [nvarchar](50) NULL,
	 [First Contact Team ID] [nvarchar](50) NULL,
	 [First Contact Team Name] [nvarchar](50) NULL,
	 [First Contact Aerial] [bit] NULL,
	 [Defending Goalkeeper ID] [nvarchar](50) NULL,
	 [Defending Goalkeeper Name] [nvarchar](50) NULL,
	 [First Contact Event ID] [nvarchar](50) NULL,
	 [First Contact Shot] [bit] NULL,
	 [First Contact X Coordinate] [float] NULL,
	 [First Contact Y Coordinate] [float] NULL

)
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



