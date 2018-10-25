using Rafty.Infrastructure;

namespace RQLite.Sharp.Raft
{
    public class RaftSettings : ISettings
    {
        public int MinTimeout { get; set; } = 1000;
        public int MaxTimeout { get; set; } = 2000;
        public int HeartbeatTimeout { get; set; } = 1000;
        public int CommandTimeout { get; set; } = 10000;
    }
}