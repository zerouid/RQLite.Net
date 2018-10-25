using System;
using System.Threading;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using RQLite.Net.Db;
using RQLite.Net.Util;

namespace RQLite.Net.Store
{
    /// <summary>
    /// Connection is a connection to the database.
    /// </summary>
    public class StoreConnection
    {
        /// <summary>
        /// // Connection ID, used as a handle by clients.
        /// </summary>
        /// <value></value>
        [JsonProperty("id")]
        public ulong ID { get; set; }
        [JsonProperty("created_at")]
        public DateTime CreatedAt { get; set; }
        [JsonProperty("last_used_at")]
        public DateTime LastUsedAt { get; set; }
        [JsonProperty("idle_timeout")]
        public TimeSpan IdleTimeout { get; set; }
        [JsonProperty("tx_timeout")]
        public TimeSpan TxTimeout { get; set; }
        [JsonProperty("tx_started_at")]
        public DateTime? TxStartedAt { get; set; }


        /// <summary>
        /// ConnectionOptions controls connection behaviour.
        /// </summary>
        public class ConnectionOptions
        {
            public TimeSpan IdleTimeout { get; set; }
            public TimeSpan TxTimeout { get; set; }
        }


        private object timeMu = new object();// sync.Mutex
        private readonly SemaphoreSlim dbMu = new SemaphoreSlim(1, 1);
        private object txStateMu = new object();
        /// <summary>
        /// Connection to SQLite database.
        /// </summary>
        private Connection db;
        /// <summary>
        /// Store to apply commands to.
        /// </summary>
        private Store store;
        private string logPrefix;
        private ILogger logger;

        /// <summary>
        /// NewConnection returns a connection to the database.
        /// </summary>
        /// <param name="c"></param>
        /// <param name="s"></param>
        /// <param name="id"></param>
        /// <param name="it"></param>
        /// <param name="tt"></param>
        public StoreConnection(Connection c, Store s, ulong id, TimeSpan it, TimeSpan tt)
        {
            var now = DateTime.Now;
            db = c;
            store = s;
            ID = id;
            CreatedAt = now;
            LastUsedAt = now;
            IdleTimeout = it;
            TxTimeout = tt;
            logger = Logging.LoggerFactory.CreateLogger(connectionLogPrefix(id));
        }

        public override string ToString()
        {
            return $"connection:{ID}";
        }

        /// <summary>
        /// Restore prepares a partially ready connection.
        /// </summary>
        /// <param name="dbConn"></param>
        /// <param name="s"></param>
        public void Restore(Connection dbConn, Store s)
        {
            dbMu.Wait();
            try
            {
                db = dbConn;
                store = s;
                logger = Logging.LoggerFactory.CreateLogger(connectionLogPrefix(ID));
            }
            finally
            {
                dbMu.Release();
            }
        }

        /// <summary>
        /// SetLastUsedNow marks the connection as being used now.
        /// </summary>
        public void SetLastUsedNow()
        {
            lock (timeMu)
            {
                LastUsedAt = DateTime.Now;
            }
        }

        /// <summary>
        /// Close closes the connection via consensus.
        /// </summary>
        public async void Close()
        {
            await dbMu.WaitAsync();
            try
            {
                if (store != null)
                {
                    await store.disconnect(this);
                }
                if (db != null)
                {
                    db.Close();
                    db = null;
                }
            }
            finally
            {
                dbMu.Release();
            }

        }

        /// <summary>
        /// IdleTimedOut returns if the connection has not been active in the idle time.
        /// </summary>
        /// <returns></returns>
        public bool IdleTimedOut()
        {
            lock (timeMu)
            {
                return IdleTimeout != TimeSpan.Zero && (DateTime.Now - LastUsedAt) > IdleTimeout;
            }
        }

        /// <summary>
        /// TxTimedOut returns if the transaction has been open, without activity in
        /// transaction-idle time.
        /// </summary>
        /// <returns></returns>
        public bool TxTimedOut()
        {
            lock (timeMu)
                lock (txStateMu)
                {
                    var lau = LastUsedAt;
                    return !TxStartedAt.HasValue && TxTimeout != TimeSpan.Zero && (DateTime.Now - lau) > TxTimeout;
                }
        }
        private string connectionLogPrefix(ulong id)
        {
            return "[connection] ";
        }
    }
}