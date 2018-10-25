using Microsoft.Extensions.Logging;
using RQLite.Sharp.Raft;

namespace RQLite.Sharp.Store
{
    /// <summary>
    /// StoreConfig represents the configuration of the underlying Store.
    /// </summary>
    public class StoreConfig
    {
        /// <summary>
        /// The DBConfig object for this Store.
        /// </summary>
        public DBConfig DBConf { get; set; }
        /// <summary>
        /// The working directory for raft.
        /// </summary>
        public string Dir { get; set; }
        /// <summary>
        /// Node ID.
        /// </summary>
        public string ID { get; set; }
        /// <summary>
        /// The logger to use to log stuff.
        /// </summary>
        public ILogger Logger { get; set; }

        /// <summary>
        /// Raft settings.
        /// </summary>
        /// <value></value>
        public RaftSettings RaftSettings { get; set; }
    }
}