using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
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
        internal Connection db;
        /// <summary>
        /// Store to apply commands to.
        /// </summary>
        public Store Store { get; private set; }
        private string logPrefix;
        public ILogger Logger { get; private set; }

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
            Store = s;
            ID = id;
            CreatedAt = now;
            LastUsedAt = now;
            IdleTimeout = it;
            TxTimeout = tt;
            Logger = Logging.LoggerFactory.CreateLogger(connectionLogPrefix(id));
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
                Store = s;
                Logger = Logging.LoggerFactory.CreateLogger(connectionLogPrefix(ID));
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
        /// TransactionActive returns whether a transaction is active on the connection.
        /// </summary>
        /// <value></value>
        public bool TransactionActive
        {
            get
            {
                dbMu.Wait();
                try
                {
                    if (db != null)
                    {
                        return db.TransactionActive;
                    }
                    else
                    {
                        return false;
                    }
                }
                finally
                {
                    dbMu.Release();
                }
            }
        }

        /// <summary>
        /// Execute executes queries that return no rows, but do modify the database.
        /// </summary>
        /// <param name="ex"></param>
        /// <returns></returns>
        public async Task<ExecuteResponse> Execute(ExecuteRequest ex)
        {
            await dbMu.WaitAsync();
            try
            {
                if (db != null)
                {
                    return await Store.execute(this, ex);
                }
                else
                {
                    throw new ConnectionDoesNotExistException();
                }
            }
            finally
            {
                dbMu.Release();
            }
        }

        /// <summary>
        /// ExecuteOrAbort executes the requests, but aborts any active transaction
        /// on the underlying database in the case of any error.
        /// </summary>
        /// <param name="ex"></param>
        /// <returns></returns>
        public async Task<ExecuteResponse> ExecuteOrAbort(ExecuteRequest ex)
        {
            await dbMu.WaitAsync();
            try
            {
                if (db != null)
                {
                    return await Store.executeOrAbort(this, ex);
                }
                else
                {
                    throw new ConnectionDoesNotExistException();
                }
            }
            finally
            {
                dbMu.Release();
            }
        }

        /// <summary>
        /// Query executes queries that return rows, and do not modify the database.
        /// </summary>
        /// <param name="qr"></param>
        /// <returns></returns>
        public async Task<QueryResponse> Query(QueryRequest qr)
        {
            await dbMu.WaitAsync();
            try
            {
                if (db != null)
                {
                    return await Store.query(this, qr);
                }
                else
                {
                    throw new ConnectionDoesNotExistException();
                }
            }
            finally
            {
                dbMu.Release();
            }

        }

        /// <summary>
        /// AbortTransaction aborts -- rolls back -- any active transaction. Calling code
        /// should know exactly what it is doing if it decides to call this function. It
        /// can be used to clean up any dangling state that may result from certain
        /// error scenarios.
        /// </summary>
        /// <returns></returns>
        public async Task AbortTransaction()
        {
            await dbMu.WaitAsync();
            try
            {
                if (db != null)
                {
                    await Store.execute(this, new ExecuteRequest { Queries = new[] { "ROLLBACK" }, Timings = false, Atomic = false });
                }
                else
                {
                    throw new ConnectionDoesNotExistException();
                }
            }
            finally
            {
                dbMu.Release();
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
                if (Store != null)
                {
                    await Store.disconnect(this);
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

        // Stats returns the status of the connection.
        public IDictionary<string, object> Stats()
        {
            dbMu.Wait();
            try
            {
                lock (timeMu)
                    lock (txStateMu)
                    {

                        var fkEnabled = db.FKConstraints();

                        var m = new Dictionary<string, object>();
                        m.Add("last_used_at", LastUsedAt);
                        m.Add("created_at", CreatedAt);

                        m.Add("idle_timeout", IdleTimeout.ToString());
                        m.Add("tx_timeout", TxTimeout.ToString());

                        m.Add("id", ID);
                        m.Add("fk_constraints", fkEnabled ? "enabled" : "disabled");
                        if (TxStartedAt.HasValue)
                        {
                            m.Add("tx_started_at", TxStartedAt.ToString());
                        }
                        return m;
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