using System.Collections.Generic;
using RQLite.Net.Db;

namespace RQLite.Net.Store
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