using System.Diagnostics.Tracing;

namespace RQLite.Sharp.Store
{
    public sealed class StoreEventCountersSource : EventSource
    {
        public EventCounter NumSnapshots { get; private set; }
        public EventCounter NumSnaphotsBlocked { get; private set; }
        public EventCounter NumBackups { get; private set; }
        public EventCounter NumRestores { get; private set; }
        public EventCounter NumConnects { get; private set; }
        public EventCounter NumDisconnects { get; private set; }
        public EventCounter NumConnTimeouts { get; private set; }
        public StoreEventCountersSource(string name) : base(name)
        {
            NumSnapshots = new EventCounter("num_snapshots", this);
            NumSnaphotsBlocked = new EventCounter("num_snapshots_blocked", this);
            NumBackups = new EventCounter("num_backups", this);
            NumRestores = new EventCounter("num_restores", this);
            NumConnects = new EventCounter("num_connects", this);
            NumDisconnects = new EventCounter("num_disconnects", this);
            NumConnTimeouts = new EventCounter("num_conn_timeouts", this);
        }
    }
}