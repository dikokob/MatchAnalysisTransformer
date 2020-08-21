using System;
using System.Collections.Generic;
using System.Text;

namespace MatchAnalysisWriteFilesToSqlServerFunction
{
	class SecondPhaseSetPieces
    {
        public static readonly List<DataTypeColumnMap> ColumnMaps = new List<DataTypeColumnMap>{
            new DataTypeColumnMap("game_id", typeof(string)),

            };


    }
}
