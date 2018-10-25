using System.Collections.Generic;
using RQLite.Sharp.Db;

namespace RQLite.Sharp.Store
{
    /// <summary>
    /// ExecuteResponse encapsulates a response to an execute.
    /// </summary>
    public class ExecuteResponse
    {
        public IEnumerable<Result> Results { get; set; }
        public double Time { get; set; }
        public RaftResponse Raft { get; set; }
    }
}