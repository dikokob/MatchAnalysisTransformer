using System;
using System.Collections.Generic;
using System.Text;

namespace MatchAnalysisWriteFilesToSqlServerFunction
{
	class Crosses
    {
        public static readonly DataTypeColumnMap game_id = new DataTypeColumnMap("game_id", typeof(string));
        public static readonly DataTypeColumnMap Fixture = new DataTypeColumnMap("Fixture", typeof(string));

        public static readonly List<DataTypeColumnMap> ColumnMaps = new List<DataTypeColumnMap>{
            game_id,
            Fixture,
            new DataTypeColumnMap("Attacking Team", typeof(string)),
            new DataTypeColumnMap("Defending Team", typeof(string)),
            new DataTypeColumnMap("Attacking Team ID", typeof(string)),
            new DataTypeColumnMap("Defending Team ID", typeof(string)),
            new DataTypeColumnMap("Goals Scored", typeof(int)),
            new DataTypeColumnMap("Goals Conceded", typeof(int)),
            new DataTypeColumnMap("Goals Difference", typeof(int)),
            new DataTypeColumnMap("Game State", typeof(string)),
            new DataTypeColumnMap("Side", typeof(string)),
            new DataTypeColumnMap("Early/Lateral/Deep", typeof(string)),
            new DataTypeColumnMap("OPTA Event ID", typeof(string)),
            new DataTypeColumnMap("perid_id", typeof(int)),
            new DataTypeColumnMap("min", typeof(int)),
            new DataTypeColumnMap("sec", typeof(int)),
            new DataTypeColumnMap("X Coordinate", typeof(float)),
            new DataTypeColumnMap("Y Coordinate", typeof(float)),
            new DataTypeColumnMap("End X Coordinate", typeof(float)),
            new DataTypeColumnMap("End Y Coordinate", typeof(float)),
            new DataTypeColumnMap("Length Pass", typeof(float)),
            new DataTypeColumnMap("% Distance Along X", typeof(float)),
            new DataTypeColumnMap("Player ID", typeof(string)),
            new DataTypeColumnMap("Player Name", typeof(string)),
            new DataTypeColumnMap("Preferred Foot", typeof(string)),
            new DataTypeColumnMap("Outcome", typeof(string)),
            new DataTypeColumnMap("Keypass/Assist", typeof(string)),
            new DataTypeColumnMap("Blocked Pass", typeof(bool)),
            new DataTypeColumnMap("Cutback", typeof(bool)),
            new DataTypeColumnMap("OPTA Pull Back Qualifier", typeof(bool)),
            new DataTypeColumnMap("Out Of Pitch", typeof(bool)),
            new DataTypeColumnMap("Ending Too Wide", typeof(bool)),
            new DataTypeColumnMap("Cross Type", typeof(string)),
            new DataTypeColumnMap("Set Piece OPTA Event ID", typeof(string)),
            new DataTypeColumnMap("OPTA Cross Qualifier", typeof(bool)),
            new DataTypeColumnMap("Time Between Set Piece and Cross", typeof(float)),
            new DataTypeColumnMap("Number Events Between Set Piece and Cross", typeof(int)),
            new DataTypeColumnMap("Linked 2nd Phase Cross IDs", typeof(string)),
            new DataTypeColumnMap("First Contact Type", typeof(int)),
            new DataTypeColumnMap("First Contact Explanation", typeof(string)),
            new DataTypeColumnMap("First Contact Player ID", typeof(string)),
            new DataTypeColumnMap("First Contact Player Name", typeof(string)),
            new DataTypeColumnMap("First Contact Team ID", typeof(string)),
            new DataTypeColumnMap("First Contact Team NAme", typeof(string)),
            new DataTypeColumnMap("First Contact Aerial", typeof(bool)),
            new DataTypeColumnMap("Defending Goalkeeper ID", typeof(string)),
            new DataTypeColumnMap("Defending Goalkeeper Name", typeof(string)),
            new DataTypeColumnMap("First Contact Event ID", typeof(string)),
            new DataTypeColumnMap("First Contact Shot", typeof(bool)),
            new DataTypeColumnMap("First Contact X Coordinate", typeof(float)),
            new DataTypeColumnMap("First Contact Y Coordinate", typeof(float))
            };


    }
}
