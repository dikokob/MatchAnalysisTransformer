CREATE TYPE [dbo].[AerialDuelsSetPiecesType] AS TABLE(
	[Set Piece OPTA Event ID] [int] NULL,
	[Aerial Duel OPTA ID] [int] NULL,
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


