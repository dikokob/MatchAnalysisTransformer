using System;
using System.Collections.Generic;
using System.Text;

namespace MatchAnalysisWriteFilesToSqlServerFunction
{
	class ShotsfromCrosses
    {   
        public static readonly DataTypeColumnMap game_id = new DataTypeColumnMap("game_id", typeof(string));
        public static readonly DataTypeColumnMap Fixture = new DataTypeColumnMap("Fixture", typeof(string));

        public static readonly List<DataTypeColumnMap> ColumnMaps = new List<DataTypeColumnMap>{
            game_id,
            Fixture,
            new DataTypeColumnMap("Cross OPTA Event ID", typeof(string)),
            new DataTypeColumnMap("Shot OPTA ID", typeof(string)),
            new DataTypeColumnMap("Shot Player ID", typeof(string)),
            new DataTypeColumnMap("Shot Player Name", typeof(string)),
            new DataTypeColumnMap("Shot Team ID", typeof(string)),
            new DataTypeColumnMap("Shot Team Name", typeof(string)),
            new DataTypeColumnMap("Shot Occurrence", typeof(string)),
            new DataTypeColumnMap("Shot Outcome", typeof(string)),
            new DataTypeColumnMap("Shot Body Part", typeof(string)),
            new DataTypeColumnMap("Aerial Duel Is Shot", typeof(bool)),
            v

            };


    }
}
