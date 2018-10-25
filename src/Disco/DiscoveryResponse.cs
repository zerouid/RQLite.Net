using System.Collections.Generic;
using Newtonsoft.Json;

namespace RQLite.Sharp.Disco
{
    /// <summary>
    /// Response represents the response returned by a Discovery Service.
    /// </summary>
    public class DiscoveryResponse
    {
        [JsonProperty("created_at")]
        public string CreatedAt { get; set; }
        [JsonProperty("disco_id")]
        public string DiscoID { get; set; }
        [JsonProperty("nodes")]
        public IEnumerable<string> Nodes { get; set; }
    }
}