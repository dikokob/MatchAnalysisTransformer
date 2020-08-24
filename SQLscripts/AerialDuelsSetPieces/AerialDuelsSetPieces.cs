using System;
using System.Collections.Generic;
using System.Text;

namespace MatchAnalysisWriteFilesToSqlServerFunction
{
	class AerialDuelsSetPieces
    {
        public static readonly List<DataTypeColumnMap> ColumnMaps = new List<DataTypeColumnMap>{
            new DataTypeColumnMap("Set Piece OPTA Event ID", typeof(int)),
            new DataTypeColumnMap("Aerial Duel OPTA ID", typeof(int)),
            new DataTypeColumnMap("Aerial Duel OPTA ID", typeof(string)),
            new DataTypeColumnMap("Aerial Duel Player Name", typeof(string)),
            new DataTypeColumnMap("Aerial Duel Team ID", typeof(string)),
            new DataTypeColumnMap("Aerial Duel Team Name", typeof(string)),
            new DataTypeColumnMap("Successfull/Unsuccessful", typeof(string)),
            new DataTypeColumnMap("Other Aerial Duel Player ID", typeof(string)),
            new DataTypeColumnMap("Other Aerial Duel Player Name", typeof(string)),
            new DataTypeColumnMap("Other Aerial Duel Team ID", typeof(string)),
            new DataTypeColumnMap("Other Aerial Duel Team Name", typeof(string)),
            new DataTypeColumnMap("Aerial Duel Is Shot", typeof(bool))
            };


    }
}
