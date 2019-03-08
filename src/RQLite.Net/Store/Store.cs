using System;
using System.Linq;
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
using RQLite.Net.Db;
using RQLite.Net.Raft;
using RQLite.Net.Util;
using Rafty.Concensus.Messages;
using System.Data.SQLite;

namespace RQLite.Net.Store
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
        private ISettings raftSettings;
        private IPeersProvider raftPeers;
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

        public ulong SnapshotThreshold { get; set; }
        public TimeSpan SnapshotInterval { get; set; }
        public TimeSpan HeartbeatTimeout { get; set; }
        public TimeSpan ApplyTimeout { get; set; }

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
            ApplyTimeout = applyTimeout;
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
                raftLog = new Raft.SQLiteLog(raftLogDbPath, new NodeId(ID), Logging.LoggerFactory);
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
                raftPeers = new PeersProvider();
                // Instantiate the Raft system.
                raft = new Node(this, raftLog, raftSettings, raftPeers, Logging.LoggerFactory);
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
        /// LeaderAddr returns the Raft address of the current leader. Returns a
        /// blank string if there is no leader.
        /// </summary>
        /// <returns></returns>
        public string LeaderAddr
        {
            get
            {
                return (raftPeers?.Get()?.SingleOrDefault(_ => _.Id == LeaderID) as Server)?.Addr ?? string.Empty;
            }
        }


        /// <summary>
        /// LeaderID returns the node ID of the Raft leader. Returns a
        /// blank string if there is no leader, or an error.
        /// </summary>
        /// <returns></returns>
        public string LeaderID
        {
            get
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
                return raft?.State?.CurrentState?.LeaderId ?? string.Empty;
            }
        }

        /// <summary>
        /// Nodes returns the slice of nodes in the cluster, sorted by ID ascending. 
        /// </summary>
        /// <returns></returns>
        public IEnumerable<Server> Nodes
        {
            get
            {
                return raftPeers?.Get()?.OrderBy(_ => _.Id)?.Cast<Server>();
                /*
                    f := s.raft.GetConfiguration()
                    if f.Error() != nil {
                        return nil, f.Error()
                    }

                    rs := f.Configuration().Servers
                    servers := make([]* Server, len(rs))
                    for i := range rs
                    {
                        servers [i] = &Server
                        {
                        ID: string(rs[i].ID),
                            Addr: string(rs[i].Address),
                        }
                    }

                    sort.Sort(Servers(servers))
                    return servers, nil
                     */
            }
        }

        /// <summary>
        /// WaitForLeader blocks until a leader is detected, or the timeout expires.
        /// </summary>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public string WaitForLeader(TimeSpan timeout)
        {
            var ct = new CancellationTokenSource(timeout).Token;
            try
            {
                return Task.Run(async () =>
                {
                    while (string.IsNullOrEmpty(LeaderAddr))
                    {
                        ct.ThrowIfCancellationRequested();
                        await Task.Delay(appliedWaitDelay, ct);
                    }
                    return LeaderAddr;
                }, ct).Result;
            }
            catch (AggregateException ex)
            {
                if (ex.InnerException is TaskCanceledException)
                {
                    throw new TimeoutException("timeout expired");
                }
                else
                {
                    throw;
                }
            }
        }

        /// <summary>
        /// WaitForAppliedIndex blocks until a given log index has been applied,
        /// or the timeout expires.
        /// </summary>
        /// <param name="idx"></param>
        /// <param name="timeout"></param>
        public void WaitForAppliedIndex(int idx, TimeSpan timeout)
        {
            var ct = new CancellationTokenSource(timeout).Token;
            try
            {
                Task.Run(async () =>
                {
                    while (raft.State.CurrentState.CommitIndex < idx)
                    {
                        ct.ThrowIfCancellationRequested();
                        await Task.Delay(appliedWaitDelay, ct);
                    }
                }, ct).Wait();
            }
            catch (AggregateException ex)
            {
                if (ex.InnerException is TaskCanceledException)
                {
                    throw new TimeoutException("timeout expired");
                }
                else
                {
                    throw;
                }
            }
        }

        /// <summary>
        /// Stats returns stats for the store.
        /// </summary>
        /// <value></value>
        public IDictionary<string, object> Stats
        {
            get
            {
                var fkEnabled = dbConn.FKConstraints();

                var dbStatus = new Dictionary<string, object>
                {
                    {"dsn", dbConf.DSN},
                    {"fk_constraints", dbConn.FKConstraints() ? "enabled" : "disabled"},
                    {"version", System.Data.SQLite.SQLiteConnection.SQLiteVersion}
                };
                if (!dbConf.Memory)
                {
                    dbStatus.Add("path", dbPath);
                    dbStatus.Add("size", new FileInfo(dbPath).Length);
                }
                else
                {
                    dbStatus.Add("path", ":memory:");
                }
                var status = new Dictionary<string, object> {
                    {"node_id",            ID},
                    {"raft",               raft.State},
                    {"addr",               Addr},
                    {"leader", new Dictionary<string, string> {
                        {"node_id",   LeaderID},
                        {"addr",      LeaderAddr}
                    }},
                    {"apply_timeout", ApplyTimeout.ToString()},
                    {"heartbeat_timeout",  HeartbeatTimeout.ToString()},
                    {"snapshot_threshold", SnapshotThreshold},
                    {"conn_poll_period",   connPollPeriod.ToString()},
                    {"metadata",           meta},
                    {"nodes",              Nodes},
                    {"dir",                Path},
                    {"sqlite3",            dbStatus},
                    {"db_conf",            dbConf},
                };

                connsMu.EnterReadLock();
                try
                {
                    status.Add("connections", conns.Where((c) => c.Key != defaultConnID)
                                                    .Select(c => c.Value.Stats())
                                                    .ToArray());
                }
                finally
                {
                    connsMu.ExitReadLock();
                }
                return status;
            }
        }

        /// <summary>
        /// Backup writes a snapshot of the underlying database to dst
        ///
        /// If leader is true, this operation is performed with a read consistency
        /// level equivalent to "weak". Otherwise no guarantees are made about the
        /// read consistency level.
        /// </summary>
        public void Backup(bool leader, BackupFormat fmt, StreamWriter dst)
        {
            restoreMu.EnterReadLock();
            try
            {
                if (leader && !IsLeader)
                {
                    throw new NotLeaderException();
                }
                switch (fmt)
                {
                    case BackupFormat.BackupBinary:
                        database(leader, dst);
                        break;
                    case BackupFormat.BackupSQL:
                        dbConn.Dump(dst);
                        break;
                    default:
                        throw new InvalidBackupFormatException();
                }
                stats.NumBackups.WriteMetric(1);
            }
            finally
            {
                restoreMu.ExitReadLock();
            }
        }

        /// <summary>
        /// Join joins a node, identified by id and located at addr, to this store.
        /// The node must be ready to respond to Raft communications at that address.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="addr"></param>
        /// <param name="metadata"></param>
        public void Join(string id, string addr, IDictionary<string, string> metadata)
        {
            logger.LogInformation($"received request to join node at {addr}");
            if (!IsLeader)
            {
                throw new NotLeaderException();
            }

            // If a node already exists with either the joining node's ID or address,
            // that node may need to be removed from the config first.
            var srv = Nodes.SingleOrDefault(s => s.Id == id || s.Addr == addr);
            if (srv != null)
            {
                // However if *both* the ID and the address are the same, the no
                // join is actually needed.
                if (srv.Addr == addr && srv.Id == id)
                {
                    logger.LogInformation($"node {id} at {addr} already member of cluster, ignoring join request");
                    return;
                }
                try
                {
                    remove(id);
                }
                catch (Exception ex)
                {
                    logger.LogError($"failed to remove node: {ex}");
                    throw;
                }
            }

            raftPeers.Get().Add(new Server { Id = id, Addr = addr });
            setMetadata(id, metadata);
            logger.LogInformation($"node at {addr} joined successfully");
        }

        /// <summary>
        /// Remove removes a node from the store, specified by ID.
        /// </summary>
        /// <param name="id"></param>
        public void Remove(string id)
        {
            logger.LogInformation($"received request to remove node {id}");

            try
            {
                remove(id);
            }
            catch (Exception ex)
            {
                logger.LogError(ex, $"failed to remove node {id}: {ex.Message}");
                throw;
            }

            logger.LogInformation($"node {id} removed successfully");
        }

        /// <summary>
        /// Metadata returns the value for a given key, for a given node ID.
        /// </summary>
        public string Metadata(string id, string key)
        {
            metaMu.EnterReadLock();
            try
            {
                if (meta.TryGetValue(id, out var m) && m.TryGetValue(key, out var v))
                {
                    return v;
                }
                else
                {
                    return string.Empty;
                }
            }
            finally
            {
                metaMu.ExitReadLock();
            }
        }

        /// <summary>
        /// SetMetadata adds the metadata md to any existing metadata for
        /// this node.
        /// </summary>

        public void SetMetadata(IDictionary<string, string> md)
        {
            setMetadata(ID, md);
        }

        /// <summary>
        /// setMetadata adds the metadata md to any existing metadata for
        /// the given node ID.
        /// </summary>
        public async void setMetadata(string id, IDictionary<string, string> md)
        {
            // Check local data first.
            Func<bool> isSame = () =>
            {
                metaMu.EnterReadLock();
                try
                {
                    if (meta.TryGetValue(id, out var m))
                    {
                        return md.All(_ => m.TryGetValue(_.Key, out var existing) && existing == _.Value);
                    }
                }
                finally
                {
                    metaMu.ExitReadLock();
                }
                return false;
            };
            if (isSame())
            {
                // Local data is same as data being pushed in,
                // nothing to do.
                return;
            }

            var cmd = new Command { Typ = CommandType.metadataSet, Sub = new metadataSetSub { RaftID = id, Data = md } };
            await raft.Accept(cmd).ThrowOnError();
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
            stats.NumDisconnects.WriteMetric(1);
        }

        /// <summary>
        /// Execute executes queries that return no rows, but do modify the database. If connection
        /// is nil then the utility connection is used.
        /// </summary>
        /// <param name="c"></param>
        /// <param name="ex"></param>
        /// <returns></returns>
        internal async Task<ExecuteResponse> execute(StoreConnection c, ExecuteRequest ex)
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
            if (f is FsmExecuteResponse<Command>)
            {
                return new ExecuteResponse
                {
                    Results = (f as FsmExecuteResponse<Command>).Results,
                    Time = (DateTime.Now - start).TotalSeconds,
                    Raft = new RaftResponse { NodeID = ID }
                };
            }
            else if (f is ErrorResponse<Command>)
            {
                throw new Exception((f as ErrorResponse<Command>).Error);
            }
            else
            {
                throw new Exception("unsupported type");
            }
        }

        internal async Task<ExecuteResponse> executeOrAbort(StoreConnection c, ExecuteRequest ex)
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
            bool errored = false;
            try
            {
                var f = await c.Store.execute(c, ex);
                errored = f.Results.Any(r => !string.IsNullOrEmpty(r.Error));
                return f;
            }
            catch (Exception e)
            {
                errored = true;
                c.Logger.LogError(e.ToString());
                throw;
            }
            finally
            {
                if (errored)
                {
                    try
                    {
                        await c.AbortTransaction();
                    }
                    catch (Exception e)
                    {
                        c.Logger.LogWarning($"WARNING: failed to abort transaction on {c}: {e.Message}");
                    }
                }
            }
        }

        /// <summary>
        /// Query executes queries that return rows, and do not modify the database. If
        /// connection is nil, then the utility connection is used.
        /// </summary>
        /// <param name="c"></param>
        /// <param name="qr"></param>
        /// <returns></returns>
        internal async Task<QueryResponse> query(StoreConnection c, QueryRequest qr)
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
            restoreMu.EnterReadLock();
            try
            {
                var start = DateTime.Now;
                if (qr.Lvl == ConsistencyLevel.Strong)
                {
                    var d = new databaseSub
                    {
                        ConnID = c.ID,
                        Atomic = qr.Atomic,
                        Queries = qr.Queries,
                        Timings = qr.Timings
                    };
                    var cmd = new Command { Typ = CommandType.query, Sub = d };
                    var f = await raft.Accept(cmd).ThrowOnError();
                    if (f is FsmQueryResponse<Command>)
                    {
                        return new QueryResponse
                        {
                            Rows = (f as FsmQueryResponse<Command>).Rows,
                            Time = (DateTime.Now - start).TotalSeconds,
                            Raft = new RaftResponse { NodeID = ID }
                        };
                    }
                    else if (f is ErrorResponse<Command>)
                    {
                        throw new Exception((f as ErrorResponse<Command>).Error);
                    }
                    else
                    {
                        throw new Exception("unsupported type");
                    }
                }

                if (qr.Lvl == ConsistencyLevel.Weak && !(raft.State is Leader))
                {
                    throw new NotLeaderException();
                }

                var r = c.db.Query(qr.Queries.ToArray(), qr.Atomic, qr.Timings);
                return new QueryResponse
                {
                    Rows = r,
                    Time = (DateTime.Now - start).TotalSeconds
                };
            }
            finally
            {
                restoreMu.ExitReadLock();
            }

        }

        /// <summary>
        /// createDatabase creates the the in-memory or file-based database.
        /// </summary>
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

        /// <summary>
        /// remove removes the node, with the given ID, from the cluster.
        /// </summary>
        /// <param name="id"></param>
        private void remove(string id)
        {
            if (!(raft.State is Leader))
            {
                throw new NotLeaderException();
            }
            /* 
                f := s.raft.RemoveServer(raft.ServerID(id), 0, 0)
                if f.Error() != nil {
                    if f.Error() == raft.ErrNotLeader {
                        return ErrNotLeader
            }
                    return f.Error()
                }

                c, err := newCommand(metadataDelete, id)
                if err != nil {
                    return err
                }
                b, err := json.Marshal(c)
                if err != nil {
                    return err
                }
                f = s.raft.Apply(b, s.ApplyTimeout)
                if e := f.(raft.Future); e.Error() != nil {
                    if e.Error() == raft.ErrNotLeader {
                        return ErrNotLeader
                    }
                    e.Error()
                }

                return nil
                */
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
        // Apply applies a Raft log entry to the database.
        public void Apply()
        {
            // 	restoreMu.RLock()
            //     defer s.restoreMu.RUnlock()

            //     var c command
            // 	if err := json.Unmarshal(l.Data, &c); err != nil {
            // 		panic(fmt.Sprintf("failed to unmarshal cluster command: %s", err.Error()))
            // 	}

            // 	switch c.Typ {
            // 	case execute, query:
            // 		var d databaseSub
            // 		if err := json.Unmarshal(c.Sub, &d); err != nil {
            // 			return &fsmGenericResponse{error: err
            // }
            // 		}

            // 		s.connsMu.RLock()
            //         conn, ok := s.conns[d.ConnID]
            //         s.connsMu.RUnlock()
            // 		if !ok {
            // 			return &fsmGenericResponse{error: ErrConnectionDoesNotExist}
            // 		}

            // 		if c.Typ == execute {
            // 			txChange := NewTxStateChange(conn)

            //             r, err := conn.db.Execute(d.Queries, d.Atomic, d.Timings)
            //             txChange.CheckAndSet()
            // 			return &fsmExecuteResponse{results: r, error: err}
            // 		}
            // 		r, err := conn.db.Query(d.Queries, d.Atomic, d.Timings)
            // 		return &fsmQueryResponse{rows: r, error: err}
            // 	case metadataSet:
            // 		var d metadataSetSub
            // 		if err := json.Unmarshal(c.Sub, &d); err != nil {
            // 			return &fsmGenericResponse{error: err}
            // 		}
            // 		func()
            // {
            //     s.metaMu.Lock()

            //             defer s.metaMu.Unlock()

            //             if _, ok:= s.meta[d.RaftID]; !ok {
            //         s.meta[d.RaftID] = make(map[string]string)

            //             }
            //     for k, v := range d.Data {
            //         s.meta[d.RaftID][k] = v

            //             }
            // }
            // ()
            // 		return &fsmGenericResponse{}
            // 	case metadataDelete:
            // 		var d string
            // 		if err := json.Unmarshal(c.Sub, &d); err != nil {
            // 			return &fsmGenericResponse{error: err}
            // 		}
            // 		func()
            // {
            //     s.metaMu.Lock()

            //             defer s.metaMu.Unlock()

            //             delete(s.meta, d)

            //         }
            // ()
            // 		return &fsmGenericResponse{}
            // 	case connect:
            // 		var d connectionSub
            // 		if err := json.Unmarshal(c.Sub, &d); err != nil {
            // 			return &fsmGenericResponse{error: err}
            // 		}

            // 		conn, err := s.db.Connect()
            // 		if err != nil {
            // 			return &fsmGenericResponse{error: err}
            // 		}
            // 		s.connsMu.Lock()
            //         s.conns[d.ConnID] = NewConnection(conn, s, d.ConnID, d.IdleTimeout, d.TxTimeout)

            //         s.connsMu.Unlock()
            // 		return d.ConnID
            // 	case disconnect:

            //         var d connectionSub
            // 		if err := json.Unmarshal(c.Sub, &d); err != nil {
            // 			return &fsmGenericResponse{error: err}
            // 		}

            // 		if d.ConnID == defaultConnID {
            // 			return &fsmGenericResponse{error: ErrDefaultConnection}
            // 		}

            // 		return func() interface{ } {
            // 			s.connsMu.Lock()
            //             defer s.connsMu.Unlock()
            //             delete(s.conns, d.ConnID)
            // 			return &fsmGenericResponse{}
            // 		}()

            // 	default:
            // 		return &fsmGenericResponse{error: fmt.Errorf("unknown command: %v", c.Typ)}
            // 	}
        }

        /// <summary>
        /// Database copies contents of the underlying SQLite file to dst
        /// </summary>
        /// <param name="leader"></param>
        /// <param name="dst"></param>
        private void database(bool leader, StreamWriter dst)
        {
            if (leader && !IsLeader)
            {
                throw new NotLeaderException();
            }

            var tmpFile = $"rqlilte-snap-{Guid.NewGuid().ToString("n")}";
            SQLiteConnection.CreateFile(tmpFile);
            using (var conn = new SQLiteConnection($"Data Source={tmpFile};Version=3"))
            {
                conn.Open();
                dbConn.Backup(new Connection(conn));
            }
            using (var of = File.OpenRead(tmpFile))
            {
                of.CopyTo(dst.BaseStream);
            }
        }

        public Task Handle(LogEntry log)
        {
            throw new NotImplementedException();
        }
    }
}