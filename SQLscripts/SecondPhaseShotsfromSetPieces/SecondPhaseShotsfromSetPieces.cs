using System;
using System.Collections.Generic;
using System.Text;

namespace MatchAnalysisWriteFilesToSqlServerFunction
{
	class SecondPhaseShotsfromSetPieces
    {   
        public static readonly DataTypeColumnMap game_id = new DataTypeColumnMap("game_id", typeof(string));
        public static readonly DataTypeColumnMap Fixture = new DataTypeColumnMap("Fixture", typeof(string));

        public static readonly List<DataTypeColumnMap> ColumnMaps = new List<DataTypeColumnMap>{
            new DataTypeColumnMap("Set Piece OPTA Event ID", typeof(string)),
            game_id,
            Fixture,
            new DataTypeColumnMap("Shot OPTA ID", typeof(string)),
            new DataTypeColumnMap("Shot Player ID", typeof(string)),
            new DataTypeColumnMap("Shot Player Name", typeof(string)),
            new DataTypeColumnMap("Shot Team ID", typeof(string)),
            new DataTypeColumnMap("Shot Team Name", typeof(string)),
            new DataTypeColumnMap("Shot Occurence", typeof(string)),
            new DataTypeColumnMap("Shot Outcome", typeof(string)),
            new DataTypeColumnMap("Shot Body Part", typeof(string)),
            new DataTypeColumnMap("Aerial Duel Is Shot", typeof(bit)),
            new DataTypeColumnMap("Preferred Foot", typeof(string)),
            new DataTypeColumnMap("2nd Phase Cross", typeof(string)),
            new DataTypeColumnMap("2nd Phase Cross OPTA Event ID", typeof(string)),
            new DataTypeColumnMap("Events Explanation Between Set Piece and Shot", typeof(string)),
            new DataTypeColumnMap("First Contact Shot", typeof(bool)),
            new DataTypeColumnMap("First Contact X Coordinate", typeof(float)),
            new DataTypeColumnMap("First Contact Y Coordinate", typeof(float)),
            new DataTypeColumnMap("Number Of Events Between Set Piece And Shot", typeof(int)),
            new DataTypeColumnMap("Shot X Coordinate", typeof(float)),
            new DataTypeColumnMap("Shot Y Coordinate", typeof(float)),
            new DataTypeColumnMap("Time Lapsed From Set Piece And Shot", typeof(int))
            };


    }
}
