using System;
using System.Collections.Generic;
using System.Text;

namespace MatchAnalysisWriteFilesToSqlServerFunction
{
	class OptaCoreStats
    {
        public static readonly DataTypeColumnMap GameID = new DataTypeColumnMap("Game ID", typeof(string));
        public static readonly DataTypeColumnMap CompetitionID = new DataTypeColumnMap("Competition ID", typeof(string));

        public static readonly List<DataTypeColumnMap> ColumnMaps = new List<DataTypeColumnMap>{
            CompetitionID,
            new DataTypeColumnMap("Competition Name", typeof(string)),
            new DataTypeColumnMap("Date", typeof(DateTime)),
            GameID,
            new DataTypeColumnMap("Home/Away", typeof(string)),
            new DataTypeColumnMap("Opposition Team Formation", typeof(string)),
            new DataTypeColumnMap("Opposition Team Formation ID", typeof(int)),
            new DataTypeColumnMap("Opposition Team ID", typeof(string)),
            new DataTypeColumnMap("Opposition Team Name", typeof(string)),
            new DataTypeColumnMap("Player ID", typeof(string)),
            new DataTypeColumnMap("Player Name", typeof(string)),
            new DataTypeColumnMap("Position ID", typeof(int)),
            new DataTypeColumnMap("Season ID", typeof(string)),
            new DataTypeColumnMap("Season Name", typeof(string)),
            new DataTypeColumnMap("Start", typeof(bool)),
            new DataTypeColumnMap("Substitute Off", typeof(bool)),
            new DataTypeColumnMap("Substitute On", typeof(bool)),
            new DataTypeColumnMap("Team Formation", typeof(string)),
            new DataTypeColumnMap("Team Formation ID", typeof(int)),
            new DataTypeColumnMap("Team ID", typeof(string)),
            new DataTypeColumnMap("Team Name", typeof(string)),
            new DataTypeColumnMap("Time Played", typeof(decimal)),
            new DataTypeColumnMap("Time In Possession", typeof(decimal)),
            new DataTypeColumnMap("Time Out Of Possession", typeof(decimal))
            };


    }
}
