using System;
using System.Collections.Generic;
using System.Text;

namespace MatchAnalysisWriteFilesToSqlServerFunction
{
	class SecondPhaseSetPieces
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
            new DataTypeColumnMap("Goal Difference", typeof(int)),
            new DataTypeColumnMap("Game State", typeof(string)),
            new DataTypeColumnMap("Side", typeof(string)),
            new DataTypeColumnMap("Number Events In Window", typeof(int)),
            new DataTypeColumnMap("Direct", typeof(string)),
            new DataTypeColumnMap("OPTA Event ID", typeof(string)),
            new DataTypeColumnMap("period_id", typeof(int)),
            new DataTypeColumnMap("min", typeof(int)),
            new DataTypeColumnMap("sec", typeof(int)),
            new DataTypeColumnMap("X Coordinate", typeof(decimal)),
            new DataTypeColumnMap("Y Coordinate", typeof(decimal)),
            new DataTypeColumnMap("End X Coordinate", typeof(decimal)),
            new DataTypeColumnMap("End Y Coordinate", typeof(decimal)),
            new DataTypeColumnMap("Player ID", typeof(string)),
            new DataTypeColumnMap("Player Name", typeof(string)),
            new DataTypeColumnMap("% Distance Along X", typeof(decimal)),
            new DataTypeColumnMap("Length Pass", typeof(decimal)),
            new DataTypeColumnMap("Relevant OPTA Event ID", typeof(string)),
            new DataTypeColumnMap("Relevant min", typeof(int)),
            new DataTypeColumnMap("Relevant sec", typeof(int)),
            new DataTypeColumnMap("Relevant X Coordinate", typeof(decimal)),
            new DataTypeColumnMap("Relevant Y Coordinate", typeof(decimal)),
            new DataTypeColumnMap("Relevant End X Coordinate", typeof(decimal)),
            new DataTypeColumnMap("Relevant End Y Coordinate", typeof(decimal)),
            new DataTypeColumnMap("Relevant Player ID", typeof(string)),
            new DataTypeColumnMap("Relevant Player Name", typeof(string)),
            new DataTypeColumnMap("Relevant % Distance Along ", typeof(decimal)),
            new DataTypeColumnMap("Relevant Length Pass", typeof(decimal)),
            new DataTypeColumnMap("Start Area Of Pitch", typeof(string)),
            new DataTypeColumnMap("Freekick Starts After Box", typeof(string)),
            new DataTypeColumnMap("Frontal/Lateral Start", typeof(string)),
            new DataTypeColumnMap("Frontal/lateral End", typeof(string)),
            new DataTypeColumnMap("Ending Side", typeof(string)),
            new DataTypeColumnMap("Time Lapsed From Stop And Start", typeof(decimal)),
            new DataTypeColumnMap("Number Of Events Between Stop and Start", typeof(int)),
            new DataTypeColumnMap("OPTA Event IDs Between Stop and Start", typeof(string)),
            new DataTypeColumnMap("Player IDs In Pass Sequence Up To Relevant", typeof(string)),
            new DataTypeColumnMap("Player Name In Pass Sequence Up To Relevant", typeof(string)),
            new DataTypeColumnMap("Rolled", typeof(bool)),
            new DataTypeColumnMap("First Contact Type", typeof(int)),
            new DataTypeColumnMap("First Contact Explanation", typeof(string)),
            new DataTypeColumnMap("First Contact Player ID", typeof(string)),
            new DataTypeColumnMap("First Contact Player Name", typeof(string)),
            new DataTypeColumnMap("First Contact Team ID", typeof(string)),
            new DataTypeColumnMap("First Contact Team Name", typeof(string)),
            new DataTypeColumnMap("First Contact Aerial", typeof(bool)),
            new DataTypeColumnMap("Defending Goalkeeper ID", typeof(string)),
            new DataTypeColumnMap("Defending Goalkeeper Name", typeof(string)),
            new DataTypeColumnMap("Set Piece Type", typeof(string)),
            new DataTypeColumnMap("Starting Delivery Type", typeof(string)),
            new DataTypeColumnMap("Actual Delivery Type", typeof(string)),
            new DataTypeColumnMap("Passed To Edge of Box", typeof(bool)),
            new DataTypeColumnMap("Passed In Behind", typeof(bool)),
            new DataTypeColumnMap("Preferred Foot", typeof(string)),
            new DataTypeColumnMap("Relevant Preferred Foot", typeof(string)),
            new DataTypeColumnMap("Time_in_Seconds", typeof(decimal)),
            new DataTypeColumnMap("Time_in_Seconds_Relavant", typeof(decimal)),
            new DataTypeColumnMap("2nd Phase Cross OPTA Event ID", typeof(string)),
            new DataTypeColumnMap("First Contact Event ID", typeof(string)),
            new DataTypeColumnMap("First Contact Shot", typeof(bool)),
            new DataTypeColumnMap("First Contact X Coordinate", typeof(float)),
            new DataTypeColumnMap("First Contact Y Coordinate", typeof(float))          

            };


    }
}
