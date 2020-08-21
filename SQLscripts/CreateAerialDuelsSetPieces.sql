SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[AerialDuelsSetPieces](
	[ID] [int] IDENTITY(1,1) NOT NULL,
    [Set Piece OPTA Event ID] [int] NULL,

 CONSTRAINT [PK_AerialDuelsSetPieces] PRIMARY KEY CLUSTERED
(
	[ID] ASC
)WITH (STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF) ON [PRIMARY]
) ON [PRIMARY]
GO
