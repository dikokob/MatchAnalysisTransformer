SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[AerialDuelsSetPieces](
	[ID] [int] IDENTITY(1,1) NOT NULL,
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
 CONSTRAINT [PK_AerialDuelsSetPieces] PRIMARY KEY CLUSTERED
(
	[ID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
