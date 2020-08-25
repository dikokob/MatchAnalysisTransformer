IF  EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[dbo].[Crosses]') AND type in (N'U'))
    DROP TABLE [dbo].[Crosses]
GO


SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[Crosses](
    [ID] [int] IDENTITY(1,1) NOT NULL,
    [game_id] [nvarchar](50) NULL,
    [Fixture] [nvarchar](50) NULL,
    [Attacking Team] [nvarchar](50) NULL,
    [Defending Team] [nvarchar](50) NULL,
    [Attacking Team ID] [nvarchar](50) NULL,
    [Defending Team ID] [nvarchar](50) NULL,
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
    [Defending Goalkeeper Name] [nvarchar](50) NULL

 CONSTRAINT [PK_Crosses] PRIMARY KEY CLUSTERED
(
	[ID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
