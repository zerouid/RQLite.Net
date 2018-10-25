using System.Collections.Generic;
using Newtonsoft.Json;

namespace RQLite.Sharp.Db
{
    /// <summary>
    /// Rows represents the outcome of an operation that returns query data.
    /// </summary>
    public class Rows
    {
        [JsonProperty("columns")]
        public string[] Columns { get; set; }

        [JsonProperty("types")]
        public string[] Types { get; set; }

        [JsonProperty("values")]
        public object[][] Values { get; set; }

        [JsonProperty("error")]
        public string Error { get; set; }

        [JsonProperty("time")]
        public double Time { get; set; }
    }
}