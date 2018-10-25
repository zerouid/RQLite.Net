using System.Collections.Generic;

namespace RQLite.Net.Store
{
    /// <summary>
    /// QueryRequest represents a query that returns rows, and does not modify
    /// the database.
    /// </summary>
    public class QueryRequest
    {
        public IEnumerable<string> Queries { get; set; }
        public bool Timings { get; set; }
        public bool Atomic { get; set; }
        public ConsistencyLevel Lvl { get; set; }
    }
}