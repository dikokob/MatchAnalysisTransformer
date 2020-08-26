SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

DROP TABLE [dbo].[SecondPhaseSetPieces]
GO

CREATE TABLE [dbo].[SecondPhaseSetPieces](
	[ID] [int] IDENTITY(1,1) NOT NULL,
    [game_id] [nvarchar](50) NULL,
	[Fixture] [nvarchar](50) NULL,
	[Attacking Team] [nvarchar](50) NULL,
	[Defending Team] [nvarchar](50) NULL,
	[Attacking Team ID] [nvarchar](50) NULL,
	[Defending Team ID] [nvarchar](50) NULL,
	[Goals Scored] [int] NULL,
	[Goals Conceded] [int] NULL,
	[Goal Difference] [int] NULL,
	[Game State] [nvarchar](50) NULL,
	[Side] [nvarchar](50) NULL,
	[Number Events In Window] [int] NULL,
	[Direct] [nvarchar](50) NULL,
	[OPTA Event ID] [nvarchar](50) NULL,
	[period_id] [int] NULL,
	[min] [int] NULL,
	[sec] [int] NULL,
	[X Coordinate] [float] NULL,
	[Y Coordinate] [float] NULL,
	[End X Coordinate] [float] NULL,
	[End Y Coordinate] [float] NULL,
	[Player ID] [nvarchar](50) NULL,
	[Player Name] [nvarchar](50) NULL,
	[% Distance Along X] [float] NULL,
	[Length Pass] [float] NULL,
	[Relevant OPTA Event ID] [nvarchar](50) NULL,
	[Relevant min] [int] NULL,
	[Relevant sec] [int] NULL,
	[Relevant X Coordinate] [float] NULL,
	[Relevant Y Coordinate] [float] NULL,
	[Relevant End X Coordinate] [float] NULL,
	[Relevant End Y Coordinate] [float] NULL,
	[Relevant Player ID] [nvarchar](50) NULL,
	[Relevant Player Name] [nvarchar](50) NULL,
	[Relevant % Distance Along X] [float] Null,
	[Relevant Length Pass] [float] NULL,
	[Start Area Of Pitch] [nvarchar](50) NULL,
	[Freekick Starts After Box] [bit] NULL,
	[Frontal/Lateral Start] [nvarchar](50) NULL,
	[Frontal/lateral End] [nvarchar](50) NULL,
	[Ending Side] [nvarchar](50) NULL,
	[Time Lapsed From Stop And Start] [float] NULL,
	[Number Of Events Between Stop and Start] [int] NULL,
	[OPTA Event IDs Between Stop and Start] [nvarchar](50) NULL,
	[Player IDs In Pass Sequence Up To Relevant] [nvarchar](50) NULL,
	[Player Name In Pass Sequence Up To Relevant] [nvarchar](50) NULL,
	[Rolled] [bit] NULL,
	[First Contact Type] [int] NULL,
	[First Contact Explanation] [nvarchar](50) NULL,
	[First Contact Player ID] [nvarchar](50) NULL,
	[First Contact Player Name] [nvarchar](50) NULL,
	[First Contact Team ID] [nvarchar](50) NULL,
	[First Contact Team Name] [nvarchar](50) NULL,
	[First Contact Aerial] [bit] NULL,
	[Defending Goalkeeper ID] [nvarchar](50) NULL,
	[Defending Goalkeeper Name] [nvarchar](50) NULL,
	[Set Piece Type] [nvarchar](50) NULL,
	[Starting Delivery Type] [nvarchar](50) NULL,
	[Actual Delivery Type] [nvarchar](50) NULL,
	[Passed To Edge of Box] [bit] NULL,
	[Passed In Behind] [bit] NULL,
	[Preferred Foot] [nvarchar](50) NULL,
	[Relevant Preferred Foot] [nvarchar](50) NULL,
	[Time_in_Seconds] [float] NULL,
	[Time_in_Seconds_Relavant] [float] NULL,
	[2nd Phase Cross OPTA Event ID] [nvarchar](50) NULL

 CONSTRAINT [PK_SecondPhaseSetPieces] PRIMARY KEY CLUSTERED
(
	[ID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
