namespace RQLite.Sharp.Store
{
    /// <summary>
    /// commandType are commands that affect the state of the cluster, and must go through Raft.
    /// </summary>
    public enum CommandType
    {
        /// <summary>Commands which modify the database.</summary>
        execute,
        /// <summary>Commands which query the database.</summary>
        query,
        /// <summary>Commands which sets Store metadata</summary>
        metadataSet,
        /// <summary>Commands which deletes Store metadata</summary>
        metadataDelete,
        /// <summary>Commands which create a database connection</summary>
        connect,
        /// <summary>Commands which disconnect from the database.</summary>
        disconnect,

    }
}