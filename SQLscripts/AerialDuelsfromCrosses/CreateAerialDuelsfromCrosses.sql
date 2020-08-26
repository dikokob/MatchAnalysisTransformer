SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DROP TABLE
[dbo].[AerialDuelsfromCrosses]
Go

CREATE TABLE [dbo].[AerialDuelsfromCrosses](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[game_id] [nvarchar](50) NULL,
	[Fixture] [nvarchar](50) NULL,
    [Cross OPTA Event ID] [nvarchar](50) NULL,
	[Aerial Duel OPTA ID] [nvarchar](50) NULL,
	[Aerial Duel Player ID] [nvarchar](50) NULL,
	[Aerial Duel Player Name] [nvarchar](50) NULL,
	[Aerial Duel Team ID] [nvarchar](50) NULL,
	[Aerial Duel Team Name] [nvarchar](50) NULL,
	[Successful/Unsuccessful] [nvarchar](50) NULL,
	[Other Aerial Duel Player ID] [nvarchar](50) NULL,
	[Other Aerial Duel Player Name] [nvarchar](50) NULL,
	[Other Aerial Duel Team ID] [nvarchar](50) NULL,
	[Other Aerial Duel Team Name] [nvarchar](50) NULL,
	[Other Aerial Duel Is Shot] [bit] NULL

 CONSTRAINT [PK_AerialDuelsfromCrosses] PRIMARY KEY CLUSTERED
(
	[ID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
