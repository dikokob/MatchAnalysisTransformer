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
            new DataTypeColumnMap("Events Explanation Between Cross and Shot", typeof(string)),
            new DataTypeColumnMap("First Contact Shot", typeof(bool)),
            new DataTypeColumnMap("First Contact X Coordinate", typeof(float)),
            new DataTypeColumnMap("First Contact Y Coordinate", typeof(float)),
            new DataTypeColumnMap("Number Of Events Between Cross And Shot", typeof(int)),
            new DataTypeColumnMap("OPTA Event IDs between Cross And Shot", typeof(string)),
            new DataTypeColumnMap("Preferred Foot", typeof(string)),
            new DataTypeColumnMap("Shot X Coordinate", typeof(float)),
            new DataTypeColumnMap("Shot Y Coordinate", typeof(float)),
            new DataTypeColumnMap("Time Lapsed from Cross And Shot", typeof(int))         
            };


    }
}
