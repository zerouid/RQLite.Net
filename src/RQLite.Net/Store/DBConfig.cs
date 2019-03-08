namespace RQLite.Net.Store
{
    /// <summary>
    /// DBConfig represents the configuration of the underlying SQLite database.
    /// </summary>
    public class DBConfig
    {
        /// <summary>
        /// Any custom DSN
        /// </summary>
        public string DSN { get; private set; }
        /// <summary>
        /// Whether the database is in-memory only.
        /// </summary>
        public bool Memory { get; private set; }

        /// <summary>
        /// NewDBConfig returns a new DB config instance.
        /// </summary>
        /// <param name="dsn">Any custom DSN</param>
        /// <param name="memory">Whether the database is in-memory only.</param>
        public DBConfig(string dsn, bool memory)
        {
            DSN = dsn;
            Memory = memory;

        }
    }
}