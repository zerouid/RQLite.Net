using System.Collections.Generic;
using RQLite.Net.Db;

namespace RQLite.Net.Store
{
    /// <summary>
    /// QueryResponse encapsulates a response to a query.
    /// </summary>
    public class QueryResponse
    {
        public IEnumerable<Rows> Rows { get; set; }
        public double Time { get; set; }
        public RaftResponse Raft { get; set; }
    }
}