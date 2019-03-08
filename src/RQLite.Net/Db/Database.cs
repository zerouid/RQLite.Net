namespace RQLite.Net.Db
{
    /// <summary>
    /// DB is the SQL database.
    /// </summary>
    public class Database
    {
        private string connString;

        /// <summary>
        /// New returns an instance of the database at path. If the database
        /// has already been created and opened, this database will share
        /// the data of that database when connected.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="dsnQuery"></param>
        /// <param name="memory"></param>
        public Database(string connString) => this.connString = connString;

        /// <summary>
        /// Connect returns a connection to the database.
        /// </summary>
        /// <returns></returns>
        public Connection Connect()
        {
            return new Connection(new System.Data.SQLite.SQLiteConnection(connString));
        }
    }
}