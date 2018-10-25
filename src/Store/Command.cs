using System;
using System.Collections.Generic;
using Newtonsoft.Json;
using Rafty.FiniteStateMachine;

namespace RQLite.Sharp.Store
{
    public class Command : ICommand
    {
        [JsonProperty("typ")]
        public CommandType Typ { get; set; }

        [JsonProperty("sub")]
        public CommandSub Sub { get; set; }
    }

    public abstract class CommandSub { }

    // databaseSub is a command sub which involves interaction with the database.
    public class databaseSub : CommandSub
    {
        [JsonProperty("conn_id")]
        public ulong ConnID { get; set; }

        [JsonProperty("atomic")]
        public bool Atomic { get; set; }

        [JsonProperty("queries")]
        public IEnumerable<string> Queries { get; set; }

        [JsonProperty("timings")]
        public bool Timings { get; set; }
    }

    public class metadataSetSub : CommandSub
    {
        [JsonProperty("raft_id")]
        public string RaftID { get; set; }

        [JsonProperty("data")]
        public IDictionary<string, string> Data { get; set; }
    }

    public class connectionSub : CommandSub
    {
        [JsonProperty("conn_id")]
        public ulong ConnID { get; set; }

        [JsonProperty("idle_timeout")]
        public TimeSpan? IdleTimeout { get; set; }

        [JsonProperty("tx_timeout")]
        public TimeSpan? TxTimeout { get; set; }
    }
}