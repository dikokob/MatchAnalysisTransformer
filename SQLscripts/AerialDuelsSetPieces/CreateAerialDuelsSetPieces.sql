SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

Drop Table [dbo].[AerialDuelsSetPieces]
Go

CREATE TABLE [dbo].[AerialDuelsSetPieces](
	[ID] [int] IDENTITY(1,1) NOT NULL,
    [game_id] [nvarchar](50) NULL,
    [Fixture] [nvarchar](50) NULL,
    [Set Piece OPTA Event ID] [nvarchar](50) NULL,
    [Aerial Duel OPTA ID] [nvarchar](50) NULL,
    [Aerial Duel Player ID] [nvarchar] (50) NULL,
	[Aerial Duel Player Name] [nvarchar] (50) Null,
	[Aerial Duel Team ID] [nvarchar] (50) Null,
	[Aerial Duel Team Name] [nvarchar] (50) NULL,
	[Successful/Unsuccessful] [nvarchar] (50) NULL,
	[Other Aerial Duel Player ID] [nvarchar] (50) NULL,
	[Other Aerial Duel Player Name] [nvarchar] (50) NULL,
	[Other Aerial Duel Team ID] [nvarchar] (50) NULL,
	[Other Aerial Duel Team Name][nvarchar] (50) NULL,
	[Aerial Duel Is Shot] [bit] NULL,
	[Other X Coordinate Player] [float] NULL,
	[Other Y Coordinate Player] [float] NULL,
	[X Coordinate Player] [float] NULL,
	[Y Coordinate Player] [float] NULL
 CONSTRAINT [PK_AerialDuelsSetPieces] PRIMARY KEY CLUSTERED
(
	[ID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
