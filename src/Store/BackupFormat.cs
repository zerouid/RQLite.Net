namespace RQLite.Sharp.Store
{
    /// <summary>
    /// BackupFormat represents the backup formats supported by the Store.
    /// </summary>
    public enum BackupFormat
    {
        /// <summary>BackupSQL is dump of the database in SQL text format.</summary>
        BackupSQL,
        /// <summary>BackupBinary is a copy of the SQLite database file.</summary>
        BackupBinary,
    }
}