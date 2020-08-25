CREATE TYPE [dbo].[AerialDuelsSetPiecesType] AS TABLE(
    [game_id] [nvarchar](50) NULL,
    [Fixture] [nvarchar](50) NULL,
	[Set Piece OPTA Event ID] [nvarchar](50) NULL,
	[Aerial Duel OPTA ID] [nvarchar](50) NULL,
	[Aerial Duel Player ID] [nvarchar] (50) NULL,
	[Aerial Duel Player Name] [nvarchar] (50) Null,
	[Aerial Duel Team ID] [nvarchar] (50) Null,
	[Aerial Duel Team Name] [nvarchar] (50) NULL,
	[Successfull/Unsuccessful] [nvarchar] (50) NULL,
	[Other Aerial Duel Player ID] [nvarchar] (50) NULL,
	[Other Aerial Duel Player Name] [nvarchar] (50) NULL,
	[Other Aerial Duel Team ID] [nvarchar] (50) NULL,
	[Other Aerial Duel Team Name][nvarchar] (50) NULL,
	[Aerial Duel Is Shot] [bit] NULL
    
)
GO


