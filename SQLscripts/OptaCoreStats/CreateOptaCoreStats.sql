SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO
CREATE TABLE [dbo].[OptaCoreStats](
	[ID] [int] IDENTITY(1,1) NOT NULL,
	[Competition ID] [nchar](10) NULL,
	[Competition Name] [nvarchar](50) NULL,
	[Date] [date] NULL,
	[Game ID] [nchar](10) NULL,
	[Home/Away] [nchar](10) NULL,
	[Opposition Team Formation] [nchar](10) NULL,
	[Opposition Team Formation ID] [int] NULL,
	[Opposition Team ID] [nchar](10) NULL,
	[Opposition Team Name] [nvarchar](50) NULL,
	[Player ID] [nchar](10) NULL,
	[Player Name] [nvarchar](50) NULL,
	[Position ID] [int] NULL,
	[Season ID] [nchar](10) NULL,
	[Season Name] [nvarchar](50) NULL,
	[Start] [bit] NULL,
	[Substitute Off] [bit] NULL,
	[Substitute On] [bit] NULL,
	[Team Formation] [nchar](10) NULL,
	[Team Formation ID] [int] NULL,
	[Team ID] [nchar](10) NULL,
	[Team Name] [nvarchar](50) NULL,
	[Time Played] [float] NULL,
	[Time In Possession] [float] NULL,
	[Time Out Of Possession] [float] NULL
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[OptaCoreStats] ADD  CONSTRAINT [PK_OptaCoreStats] PRIMARY KEY CLUSTERED
(
	[ID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ONLINE = OFF) ON [PRIMARY]
GO
