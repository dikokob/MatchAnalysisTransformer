SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

Drop Table [dbo].[ShotsfromCrosses]
Go

CREATE TABLE [dbo].[ShotsfromCrosses](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[game_id] [nvarchar](50) NULL,
	[Fixture] [nvarchar](50) NULL,
    [Cross OPTA Event ID] [nvarchar](50) NULL,
	[Shot OPTA ID] [nvarchar](50) NULL,
	[Shot Player ID] [nvarchar](50) NULL,
	[Shot Player Name] [nvarchar](50) NULL,
	[Shot Team ID] [nvarchar](50) NULL,
	[Shot Team Name] [nvarchar](50) NULL,
	[Shot Occurrence] [nvarchar](50) NULL,
	[Shot Outcome] [nvarchar](50) NULL,
	[Shot Body Part] [nvarchar](50) NULL,
	[Aerial Duel Is Shot] [bit] NULL,
	[Events Explanation Between Set Piece and Shot] [nvarchar](100) NULL,
	[First Contact Shot] [bit] NULL,
	[First Contact X Coordinate] [float] NULL,
	[First Contact Y Coordinate] [float] NULL,
	[Number Of Events Between Cross And Shot] [int] NULL,
	[OPTA Event IDs between Cross And Shot] [nvarchar](100) NULL,
	[Preferred Foot] [nvarchar](50) NULL,
	[Shot X Coordinate] [float] NULL,
	[Shot Y Coordinate] [float] NULL,
	[Time Lapsed from Cross And Shot] [int] NULL

 CONSTRAINT [PK_ShotsfromCrosses] PRIMARY KEY CLUSTERED
(
	[ID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
