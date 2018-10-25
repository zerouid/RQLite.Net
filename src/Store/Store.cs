using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Security.AccessControl;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Rafty.Concensus.Node;
using Rafty.Concensus.States;
using Rafty.FiniteStateMachine;
using Rafty.Infrastructure;
using Rafty.Log;
using RQLite.Sharp.Db;
using RQLite.Sharp.Raft;
using RQLite.Sharp.Util;

namespace RQLite.Sharp.Store
{
    /// <summary>
    /// Store is a SQLite database, where all changes are made via Raft consensus.
    /// </summary>
    public class Store : IFiniteStateMachine
    {
        #region Constants            
        private static readonly TimeSpan connectionPollPeriod = TimeSpan.FromSeconds(10);
        private static readonly int retainSnapshotCount = 2;
        private static readonly TimeSpan applyTimeout = TimeSpan.FromSeconds(10);
        private static readonly TimeSpan openTimeout = TimeSpan.FromSeconds(120);
        private static readonly string sqliteFile = "db.sqlite";
        private static readonly TimeSpan leaderWaitDelay = TimeSpan.FromMilliseconds(100);
        private static readonly TimeSpan appliedWaitDelay = TimeSpan.FromMilliseconds(100);
        private static readonly int connectionPoolCount = 5;
        private static readonly TimeSpan connectionTimeout = TimeSpan.FromSeconds(10);
        private static readonly int raftLogCacheSize = 512;
        private const int defaultConnID = 0;
        #endregion
        /// <summary>
        /// stats captures stats for the Store.
        /// </summary>
        private static readonly StoreEventCountersSource stats = new StoreEventCountersSource("store");

        private INode raft;//* raft.Raft // The consensus mechanism.
        private DBConfig dbConf; // SQLite database config.
        private RaftSettings raftSettings;
        private string dbPath;    // Path to underlying SQLite file, if not in-memory.
        private ReaderWriterLockSlim connsMu; // sync.RWMutex
        private Random randSrc;
        private Database db;               // The underlying SQLite database.
        private IDictionary<ulong, StoreConnection> conns; // map[uint64]* Connection // Database connections under management.
        private Connection dbConn;//* sdb.Conn              // Hidden connection to underlying SQLite database.
        private Rafty.Log.ILog raftLog;         // Persistent log store.
        private Barrier wg = new Barrier(0);// sync.WaitGroup
        private Channel<object> done;// chan struct{ }
        private ReaderWriterLockSlim metaMu;// sync.RWMutex
        private IDictionary<string, IDictionary<string, string>> meta;// map[string]map[string] string
        private object closedMu = new object();// sync.Mutex
        private bool closed; // Has the store been closed?
        private ReaderWriterLockSlim restoreMu;// sync.RWMutex // Restore needs exclusive access to database.
        private ILogger logger;
        private TimeSpan connPollPeriod;
        private IListener ln;
        private Transport raftTn;

        /// <summary>
        /// IsLeader is used to determine if the current node is cluster leader
        /// </summary>
        /// <value></value>
        public bool IsLeader { get { return raft.State is Leader; } }

        /// <summary>
        /// State returns the current node's Raft state
        /// </summary>
        /// <value></value>
        public ClusterState State
        {
            get
            {
                switch (raft.State)
                {
                    case Leader l:
                        return ClusterState.Leader;
                    case Candidate c:
                        return ClusterState.Candidate;
                    case Follower f:
                        return ClusterState.Follower;
                    // case Shutdown s:
                    // return ClusterState.Shutdown;
                    default:
                        return ClusterState.Unknown;

                }
            }
        }

        /// <summary>
        /// Path returns the path to the store's storage directory.
        /// </summary>
        /// <value></value>
        public string Path { get; private set; }

        /// <summary>
        /// Addr returns the address of the store.
        /// </summary>
        public string Addr => raftTn.Addr;

        /// <summary>
        /// ID returns the Raft ID of the store.
        /// </summary>
        /// <value></value>
        public string ID { get; private set; }

        /// <summary>
        /// LeaderAddr returns the Raft address of the current leader. Returns a
        /// blank string if there is no leader.
        /// </summary>
        /// <returns></returns>
        public string LeaderAddr => throw new NotImplementedException();

        public Store(IListener listener, StoreConfig c)
        {
            ln = listener;
            logger = c.Logger ?? Logging.LoggerFactory.CreateLogger("[store]");
            Path = c.Dir;
            ID = c.ID;
            dbConf = c.DBConf;
            raftSettings = c.RaftSettings;
            dbPath = System.IO.Path.Combine(c.Dir, sqliteFile);
            randSrc = new Random((int)DateTime.Now.Ticks);
            conns = new Dictionary<ulong, StoreConnection>();
            done = Channel.CreateBounded<object>(1);//make(chan struct{ }, 1)
            meta = new Dictionary<string, IDictionary<string, string>>();
            connPollPeriod = connectionPollPeriod;
        }
        /// <summary>
        /// Open opens the store. If enableSingle is set, and there are no existing peers,
        /// then this node becomes the first node, and therefore leader, of the cluster.
        /// </summary>
        /// <param name="enableSingle"></param>
        public void Open(bool enableSingle)
        {
            lock (closedMu)
            {
                if (closed) { throw new StoreInvalidStateException(); }
                logger.LogInformation($"opening store with node ID {ID}");
                logger.LogInformation($"ensuring directory at {Path} exists");
                var di = Directory.CreateDirectory(Path);
                //di.GetAccessControl().SetAccessRule(new FileSystemAccessRule()) //TODO: Set perms to 0755

                // Create underlying database.
                createDatabase();


                // Get utility connection to database.
                dbConn = db.Connect();

                conns.Add(defaultConnID, new StoreConnection(dbConn, this, defaultConnID, TimeSpan.Zero, TimeSpan.Zero));

                // Is this a brand new node?
                var raftLogDbPath = System.IO.Path.Combine(Path, "raft.db");
                var newNode = !File.Exists(raftLogDbPath);
                raftLog = new SQLiteLog(raftLogDbPath, new NodeId(ID), Logging.LoggerFactory);
                /* 
                                // Create Raft-compatible network layer.
                                raftTn = raft.NewNetworkTransport(NewTransport(ln),
                        connectionPoolCount, connectionTimeout, nil);

                                // Get the Raft configuration for this store.
                                var config = raftConfig();

                                config.LocalID = raft.ServerID(s.raftID);

                                config.Logger = log.New(os.Stderr, "[raft] ", log.LstdFlags);

                                // Create the snapshot store. This allows Raft to truncate the log.
                                snapshots, err:= raft.NewFileSnapshotStore(s.raftDir, retainSnapshotCount, os.Stderr)

                    if err != nil {
                                    return fmt.Errorf("file snapshot store: %s", err)

                    }

                                // Create the log store and stable store.
                                s.boltStore, err = raftboltdb.NewBoltStore(filepath.Join(s.raftDir, "raft.db"))

                    if err != nil {
                                    return fmt.Errorf("new bolt store: %s", err)

                    }
                                s.raftStable = s.boltStore

                    s.raftLog, err = raft.NewLogCache(raftLogCacheSize, s.boltStore)

                    if err != nil {
                                    return fmt.Errorf("new cached store: %s", err)

                    }
                    */
                var peersProvider = new PeersProvider();
                // Instantiate the Raft system.
                raft = new Node(this, raftLog, raftSettings, peersProvider, Logging.LoggerFactory);
                // ra, err:= raft.NewRaft(config, s, s.raftLog, s.raftStable, snapshots, s.raftTn)

                //     if err != nil {
                //     return fmt.Errorf("new raft: %s", err)

                //     }
                /*

                            if enableSingle && newNode {
                                s.logger.Printf("bootstrap needed")


                    configuration:= raft.Configuration{
                                Servers: []raft.Server{
                                        {
                                        ID: config.LocalID,
                                Address: raftTn.LocalAddr(),
                            },
                        },
                    }
                                ra.BootstrapCluster(configuration)


                }
                            else
                            {
                                logger.Printf("no bootstrap needed")


                }

                            raft = ra
                */
                // Start connection monitoring
                checkConnections();

            }

        }

        /// <summary>
        /// Connect returns a new connection to the database. Changes made to the database
        /// through this connection are applied via the Raft consensus system. The Store
        /// must have been opened first. Must be called on the leader or an error will
        /// we returned.
        /// 
        /// Any connection returned by this call are READ_COMMITTED isolated from all
        /// other connections, including the connection built-in to the Store itself.
        /// </summary>
        /// <param name="opt"></param>
        /// <returns></returns>
        public async Task<StoreConnection> Connect(StoreConnection.ConnectionOptions opt)
        {
            // Randomly-selected connection ID must be part of command so
            // that all nodes use the same value as connection ID.
            Func<ulong> newRandomId = () =>
            {
                var buf = new byte[8];
                randSrc.NextBytes(buf);
                return BitConverter.ToUInt64(buf, 0);
            };
            ulong connID = newRandomId();
            connsMu.EnterWriteLock();
            try
            {
                while (conns.ContainsKey(connID))
                {
                    connID = newRandomId();
                }
                conns.Add(connID, null);
            }
            finally
            {
                connsMu.ExitWriteLock();
            }
            var cmd = new Command
            {
                Typ = CommandType.connect,
                Sub = new connectionSub
                {
                    ConnID = connID,
                    IdleTimeout = opt?.IdleTimeout,
                    TxTimeout = opt?.TxTimeout
                }
            };

            var f = await raft.Accept(cmd).ThrowOnError();

            connsMu.EnterReadLock();
            try
            {
                stats.NumConnects.WriteMetric(1);
                return conns[connID];
            }
            finally
            {
                connsMu.ExitReadLock();
            }
        }

        /// <summary>
        /// Execute executes queries that return no rows, but do modify the database.
        /// Changes made to the database through this call are applied via the Raft
        /// consensus system. The Store must have been opened first. Must be called
        /// on the leader or an error will we returned. The changes are made using
        /// the database connection built-in to the Store.
        /// </summary>
        /// <param name="ex"></param>
        /// <returns></returns>
        public async Task<ExecuteResponse> Execute(ExecuteRequest ex)
        {
            return await execute(null, ex);
        }

        /// <summary>
        /// ExecuteOrAbort executes the requests, but aborts any active transaction
        /// on the underlying database in the case of any error. Any changes are made
        /// using the database connection built-in to the Store.
        /// </summary>
        /// <param name="ex"></param>
        /// <returns></returns>
        public async Task<ExecuteResponse> ExecuteOrAbort(ExecuteRequest ex)
        {
            return await executeOrAbort(null, ex);
        }

        /// <summary>
        /// Query executes queries that return rows, and do not modify the database.
        /// The queries are made using the database connection built-in to the Store.
        /// Depending on the read consistency requested, it may or may not need to be
        /// called on the leader.
        /// </summary>
        /// <param name="qr"></param>
        /// <returns></returns>
        public async Task<QueryResponse> Query(QueryRequest qr)
        {
            return await query(null, qr);
        }

        /// <summary>
        /// Close closes the store. If wait is true, waits for a graceful shutdown.
        /// Once closed, a Store may not be re-opened.
        /// </summary>
        /// <param name="wait"></param>
        public void Close(bool wait)
        {
            lock (closedMu)
            {
                if (closed)
                {
                    return;
                }
                done.Writer.TryComplete();
                wg.SignalAndWait();

                // XXX CLOSE OTHER CONNECTIONS
                dbConn.Close();
                dbConn = null;
                db = null;

                if (raft != null)
                {
                    raft.Stop();
                    raft = null;
                }

                raftLog = null;

                closed = true;
            }

        }

        /// <summary>
        /// WaitForApplied waits for all Raft log entries to to be applied to the
        /// underlying database.
        /// </summary>
        /// <param name="timeout"></param>
        public void WaitForApplied(TimeSpan timeout)
        {
            if (timeout == TimeSpan.Zero)
            {
                return;
            }
            logger.LogInformation($"waiting for up to {timeout} for application of initial logs");
            WaitForAppliedIndex(raft.State.CurrentState.LastApplied, timeout);
        }

        /// <summary>
        /// Connection returns the connection for the given ID.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="conn"></param>
        /// <returns></returns>
        public bool Connection(ulong id, out StoreConnection conn)
        {
            connsMu.EnterReadLock();
            try
            {
                return conns.TryGetValue(id, out conn);
            }
            finally
            {
                connsMu.ExitReadLock();
            }
        }

        /// <summary>
        /// LeaderID returns the node ID of the Raft leader. Returns a
        /// blank string if there is no leader, or an error.
        /// </summary>
        /// <returns></returns>
        public string LeaderID()
        {
            /*
            addr := s.LeaderAddr()
                configFuture := s.raft.GetConfiguration()
                if err := configFuture.Error(); err != nil {
                    s.logger.Printf("failed to get raft configuration: %v", err)
                    return "", err
                }

                for _, srv := range configFuture.Configuration().Servers {
                    if srv.Address == raft.ServerAddress(addr) {
                        return string(srv.ID), nil
                    }
                }
                return "", nil */
        }

        /// <summary>
        /// disconnect removes a connection to the database, a connection
        /// which was previously established via Raft consensus.
        /// </summary>
        /// <param name="c"></param>
        internal async Task disconnect(StoreConnection c)
        {
            var cmd = new Command { Typ = CommandType.disconnect, Sub = new connectionSub { ConnID = c.ID } };
            await raft.Accept(cmd).ThrowOnError();
        }

        /// <summary>
        /// Execute executes queries that return no rows, but do modify the database. If connection
        /// is nil then the utility connection is used.
        /// </summary>
        /// <param name="c"></param>
        /// <param name="ex"></param>
        /// <returns></returns>
        private async Task<ExecuteResponse> execute(StoreConnection c, ExecuteRequest ex)
        {
            if (c == null)
            {
                connsMu.EnterReadLock();
                try
                {
                    c = conns[defaultConnID];
                }
                finally
                {
                    connsMu.ExitReadLock();
                }
            }
            c.SetLastUsedNow();
            var start = DateTime.Now;
            var cmd = new Command
            {
                Typ = CommandType.execute,
                Sub = new databaseSub
                {
                    ConnID = c.ID,
                    Atomic = ex.Atomic,
                    Queries = ex.Queries,
                    Timings = ex.Timings
                }
            };

            var f = await raft.Accept(cmd).ThrowOnError();

        }

        /// <summary>
        /// checkConnections periodically checks which connections should
        /// close due to timeouts.
        /// </summary>
        private void checkConnections()
        {
            wg.AddParticipant();
            var ticker = new Timer((state) =>
            {
                if (!IsLeader)
                {
                    return;
                }
                var tmpConns = new List<StoreConnection>();
                connsMu.EnterReadLock();
                try
                {
                    foreach (var c in conns.Values)
                    {
                        // Sometimes IDs are in the slice without a connection, if a
                        // connection is in process of being formed via consensus.
                        if (c == null || c.ID == defaultConnID)
                        {
                            continue;

                        }
                        if (c.IdleTimedOut() || c.TxTimedOut())
                        {
                            tmpConns.Add(c);

                        }
                    }
                }
                finally
                {
                    connsMu.ExitReadLock();
                }
                foreach (var c in tmpConns)
                {
                    try
                    {
                        c.Close();
                        logger.LogInformation($"{c} closed due to timeout");
                        // Only increment stat here to make testing easier.
                        stats.NumConnTimeouts.WriteMetric(1);
                    }
                    catch (NotLeaderException)
                    {
                        // Not an issue, the actual leader will close it.
                        continue;
                    }
                    catch (Exception ex)
                    {
                        logger.LogError($"{c} failed to close: {ex.Message}");
                    }
                }

            }, null, TimeSpan.Zero, connectionPollPeriod);

            done.Reader.WaitToReadAsync().AsTask().ContinueWith(_ =>
            {
                wg.RemoveParticipant();
                ticker.Change(Timeout.Infinite, Timeout.Infinite);
            });
        }
        private void createDatabase()
        {
            if (!dbConf.Memory)
            {
                // as it will be rebuilt from (possibly) a snapshot and committed log entries.
                File.Delete(dbPath);
                db = new Database($"Data Source={dbPath};Version=3");
                logger.LogInformation($"SQLite database opened at {dbPath}");
            }
            else
            {
                db = new Database("Data Source=:memory:;Version=3;New=True;");
                logger.LogInformation("SQLite in-memory database opened");
            }
        }

        public Task Handle(LogEntry log)
        {
            throw new NotImplementedException();
        }
    }
}