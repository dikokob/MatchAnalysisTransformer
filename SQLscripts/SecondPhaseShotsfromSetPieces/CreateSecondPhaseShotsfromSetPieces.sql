SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

Drop TABLE
[dbo].[SecondPhaseShotsfromSetPieces]
Go

CREATE TABLE [dbo].[SecondPhaseShotsfromSetPieces](
	[ID] [int] IDENTITY(1,1) NOT NULL,
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

 CONSTRAINT [PK_SecondPhaseShotsfromSetPieces] PRIMARY KEY CLUSTERED
(
	[ID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
