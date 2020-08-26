Drop Type
[dbo].[SecondPhaseShotsfromSetPiecesType]
Go

CREATE TYPE [dbo].[SecondPhaseShotsfromSetPiecesType] AS TABLE(
	[game_id] [nvarchar](50) NULL,
	[Fixture] [nvarchar](50) NULL,
	[Set Piece OPTA Event ID] [nvarchar](50) NULL,
	[Shot OPTA ID] [nvarchar](50) NULL,
	[Shot Player ID] [nvarchar](50) NULL,
	[Shot Player Name] [nvarchar](50) NULL,
	[Shot Team ID] [nvarchar](50) NULL,
	[Shot Team Name] [nvarchar](50) NULL,
	[Shot Occurrence] [nvarchar](50) NULL,
	[Shot Outcome] [nvarchar](50) NULL,
	[Shot Body Part] [nvarchar](50) NULL,
	[Aerial Duel Is Shot] [bit] NULL,
	[Preferred Foot] [nvarchar](50) NULL,
	[2nd Phase Cross] [nvarchar](50) NULL,
	[2nd Phase Cross OPTA Event ID] [nvarchar](50) NULL


)
GO


