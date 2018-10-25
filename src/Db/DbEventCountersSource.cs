using System.Diagnostics.Tracing;

namespace RQLite.Sharp.Db
{
    public sealed class DbEventCountersSource : EventSource
    {
        public EventCounter NumExecutions { get; private set; }
        public EventCounter NumExecutionErrors { get; private set; }
        public EventCounter NumQueries { get; private set; }
        public EventCounter NumETx { get; private set; }
        public EventCounter NumQTx { get; private set; }
        public DbEventCountersSource(string name) : base(name)
        {
            NumExecutions = new EventCounter("executions", this);
            NumExecutionErrors = new EventCounter("execution_errors", this);
            NumQueries = new EventCounter("queries", this);
            NumETx = new EventCounter("execute_transactions", this);
            NumQTx = new EventCounter("query_transactions", this);
        }
    }
}