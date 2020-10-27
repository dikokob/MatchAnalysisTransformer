using System;
using System.Collections.Generic;
using System.Text;

namespace MatchAnalysisWriteFilesToSqlServerFunction
{
	class AerialDuelsfromCrosses
    {   
        public static readonly DataTypeColumnMap game_id = new DataTypeColumnMap("game_id", typeof(string));
        public static readonly DataTypeColumnMap Fixture = new DataTypeColumnMap("Fixture", typeof(string));

        public static readonly List<DataTypeColumnMap> ColumnMaps = new List<DataTypeColumnMap>{
            game_id,
            Fixture,
            new DataTypeColumnMap("Cross OPTA Event ID", typeof(string)),
            new DataTypeColumnMap("Aerial Duel OPTA ID", typeof(string)),
            new DataTypeColumnMap("Aerial Duel Player ID", typeof(string)),
            new DataTypeColumnMap("Aerial Duel Player Name", typeof(string)),
            new DataTypeColumnMap("Aerial Duel Team ID", typeof(string)),
            new DataTypeColumnMap("Aerial Duel Team Name", typeof(string)),
            new DataTypeColumnMap("Successful/Unsuccessful", typeof(string)),
            new DataTypeColumnMap("Other Aerial Duel Player ID", typeof(string)),
            new DataTypeColumnMap("Other Aerial Duel Player Name", typeof(string)),
            new DataTypeColumnMap("Other Aerial Duel Team ID", typeof(string)),
            new DataTypeColumnMap("Other Aerial Duel Is Shot", typeof(bit)),
            new DataTypeColumnMap("Other X Coordinate Player", typeof(float)),
            new DataTypeColumnMap("Other Y Coordinate Player", typeof(float)),
            new DataTypeColumnMap("X Coordinate Player", typeof(float)),
            new DataTypeColumnMap("Y Coordinate Player", typeof(float))
            };


    }
}
