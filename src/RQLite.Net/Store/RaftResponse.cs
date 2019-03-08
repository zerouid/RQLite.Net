using Newtonsoft.Json;

namespace RQLite.Net.Store
{
    /// <summary>
    /// RaftResponse is the Raft metadata that will be included with responses, if
    /// the associated request modified the Raft log.
    /// </summary>
    public class RaftResponse
    {
        [JsonProperty("index")]
        public ulong Index { get; set; }
        [JsonProperty("node_id")]
        public string NodeID { get; set; }
    }
}