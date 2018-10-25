using System.Collections.Generic;

namespace RQLite.Sharp.Store
{
    /// <summary>
    /// ExecuteRequest represents a query that returns now rows, but does modify
    /// the database.
    /// </summary>
    public class ExecuteRequest
    {
        public IEnumerable<string> Queries { get; set; }
        public bool Timings { get; set; }
        public bool Atomic { get; set; }

    }
}