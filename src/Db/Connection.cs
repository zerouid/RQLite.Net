using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Text;

namespace RQLite.Sharp.Db
{
    /// <summary>
    /// Conn represents a connection to a database. Two Connection objects
    /// to the same database are READ_COMMITTED isolated.
    /// </summary>
    public class Connection
    {
        const string fkChecks = "PRAGMA foreign_keys";
        const string fkChecksEnabled = "PRAGMA foreign_keys=ON";
        const string fkChecksDisabled = "PRAGMA foreign_keys=OFF";

        const int bkDelay = 250;

        private static readonly DbEventCountersSource stats = new DbEventCountersSource("db");
        private SQLiteConnection sqlite;

        public Connection(SQLiteConnection conn) => sqlite = conn;

        /// <summary>
        /// TransactionActive returns whether a transaction is currently active
        /// i.e. if the database is NOT in autocommit mode.
        /// </summary>
        /// <returns></returns>
        public bool TransactionActive { get { return !(sqlite?.AutoCommit == true); } }

        /// <summary>
        /// AbortTransaction aborts -- rolls back -- any active transaction. Calling code
        /// should know exactly what it is doing if it decides to call this function. It
        /// can be used to clean up any dangling state that may result from certain
        /// error scenarios.
        /// </summary>
        public void AbortTransaction()
        {
            Execute(new[] { "ROLLBACK" }, false, false);
        }

        /// <summary>
        /// Execute executes queries that modify the database.
        /// </summary>
        /// <param name="queries"></param>
        /// <param name="tx"></param>
        /// <param name="xTime"></param>
        /// <returns></returns>
        public Result[] Execute(string[] queries, bool tx, bool xTime)
        {
            stats.NumExecutions.WriteMetric(queries.Length);
            if (tx)
            {
                stats.NumETx.WriteMetric(1);
            }

            var allResults = new List<Result>();
            SQLiteTransaction t = null;
            bool rollback = false;

            try
            {
                if (tx)
                {
                    t = sqlite.BeginTransaction();
                }
                foreach (var q in queries)
                {
                    if (string.IsNullOrEmpty(q))
                    {
                        continue;
                    }
                    var result = new Result();
                    var start = DateTime.Now;
                    try
                    {
                        using (var cmd = sqlite.CreateCommand())
                        {
                            cmd.CommandText = q;
                            var r = cmd.ExecuteNonQuery();
                            if (r == 0)
                            {
                                continue;
                            }

                            result.LastInsertID = sqlite.LastInsertRowId;
                            result.RowsAffected = r;
                        }
                    }
                    catch (Exception ex)
                    {
                        stats.NumExecutionErrors.WriteMetric(1);
                        result.Error = ex.Message;
                        allResults.Add(result);
                        if (tx)
                        {
                            rollback = true;
                            throw;
                        }
                        else
                        {
                            continue;
                        }
                    }
                    finally
                    {
                        if (xTime)
                        {
                            result.Time = DateTime.Now.Subtract(start).TotalSeconds;
                        }
                        allResults.Add(result);
                    }
                }
            }
            finally
            {
                if (t != null)
                {
                    if (rollback)
                    {
                        t.Rollback();
                    }
                    else
                    {
                        t.Commit();
                    }
                }
            }
            return allResults.ToArray();
        }

        /// <summary>
        /// Query executes queries that return rows, but don't modify the database.
        /// </summary>
        /// <param name="queries"></param>
        /// <param name="tx"></param>
        /// <param name="xTime"></param>
        /// <returns></returns>
        public Rows[] Query(string[] queries, bool tx, bool xTime)
        {
            stats.NumQueries.WriteMetric(queries.Length);
            if (tx)
            {
                stats.NumQTx.WriteMetric(1);
            }

            var allRows = new List<Rows>();
            SQLiteTransaction t = null;
            bool rollback = false;

            try
            {
                if (tx)
                {
                    t = sqlite.BeginTransaction();
                }
                foreach (var q in queries)
                {
                    if (string.IsNullOrEmpty(q))
                    {
                        continue;
                    }
                    var rows = new Rows();
                    var rowValues = new List<object[]>();
                    var start = DateTime.Now;
                    try
                    {
                        using (var cmd = sqlite.CreateCommand())
                        {
                            cmd.CommandText = q;
                            var rs = cmd.ExecuteReader();

                            rows.Columns = Enumerable.Range(0, rs.FieldCount).Select(i => rs.GetName(i)).ToArray();
                            var types = Enumerable.Range(0, rs.FieldCount).Select(i => rs.GetDataTypeName(i)).ToArray();
                            rows.Types = types;
                            while (rs.Read())
                            {
                                var values = new object[rs.FieldCount];
                                rs.GetValues(values);
                                //Not needed for .NET driver
                                //values = normalizeRowValues(values, types);
                                rowValues.Add(values);
                            }
                            rows.Values = rowValues.ToArray();
                        }
                    }
                    catch (Exception ex)
                    {
                        rows.Error = ex.Message;
                        allRows.Add(rows);

                        if (tx)
                        {
                            rollback = true;
                            throw;
                        }
                        else
                        {
                            continue;
                        }
                    }
                    finally
                    {
                        if (xTime)
                        {
                            rows.Time = DateTime.Now.Subtract(start).TotalSeconds;
                        }
                        allRows.Add(rows);
                    }
                }
            }
            finally
            {
                if (t != null)
                {
                    // XXX THIS DOESN'T ACTUALLY WORK! Might as WELL JUST COMMIT?
                    if (rollback)
                    {
                        t.Rollback();
                    }
                    else
                    {
                        t.Commit();
                    }
                }
            }
            return allRows.ToArray();
        }

        /// <summary>
        /// EnableFKConstraints allows control of foreign key constraint checks.
        /// </summary>
        /// <param name="e"></param>
        public void EnableFKConstraints(bool e)
        {
            using (var cmd = sqlite.CreateCommand())
            {
                cmd.CommandText = e ? fkChecksEnabled : fkChecksEnabled;
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// FKConstraints returns whether FK constraints are set or not.
        /// </summary>
        /// <returns></returns>
        public bool FKConstraints()
        {
            using (var cmd = sqlite.CreateCommand())
            {
                cmd.CommandText = fkChecks;
                return (int)cmd.ExecuteScalar() == 1;
            }
        }

        /// <summary>
        /// Load loads the connected database from the database connected to src.
        /// It overwrites the data contained in this database. It is the caller's
        /// responsibility to ensure that no other connections to this database
        /// are accessed while this operation is in progress.
        /// </summary>
        /// <param name="src"></param>
        public void Load(Connection src)
        {
            copyDatabase(this.sqlite, src.sqlite);
        }

        /// <summary>
        /// Backup writes a snapshot of the database over the given database
        /// connection, erasing all the contents of the destination database.
        /// The consistency of the snapshot is READ_COMMITTED relative to any
        /// other connections currently open to this database. The caller must
        /// ensure that all connections to the destination database are not
        /// accessed during this operation.
        /// </summary>
        /// <param name="dst"></param>
        public void Backup(Connection dst)
        {
            copyDatabase(dst.sqlite, this.sqlite);
        }

        /// <summary>
        /// Dump writes a snapshot of the database in SQL text format. The consistency
        /// of the snapshot is READ_COMMITTED relative to any other connections
        /// currently open to this database.
        /// </summary>
        /// <param name="w"></param>
        public void Dump(TextWriter w)
        {
            w.Write("PRAGMA foreign_keys=OFF;\nBEGIN TRANSACTION;\n");
            // Get the schema.
            var query = @"SELECT ""name"", ""type"", ""sql"" FROM ""sqlite_master""
              WHERE ""sql"" NOT NULL AND ""type"" == 'table' ORDER BY ""name""";
            var rows = Query(new[] { query }, false, false);
            var row = rows[0];
            foreach (var v in row.Values)
            {
                var table = v[0] as string;
                string stmt = null;
                if (table == "sqlite_sequence")
                {
                    stmt = @"DELETE FROM ""sqlite_sequence"";";
                }
                else if (table == "sqlite_stat1")
                {
                    stmt = @"ANALYZE ""sqlite_master"";";
                }
                else if (table.StartsWith("sqlite_"))
                {
                    continue;
                }
                else
                {
                    stmt = v[1] as string;
                }

                w.Write($"{stmt};\n");

                var tableIndent = table.Replace("\"", "\"\"");
                query = $@"PRAGMA table_info(""{tableIndent}"")";
                var r = Query(new[] { query }, false, false);
                var columnNames = r[0].Values.Select(n => $@"'||quote(""{n[1]}"")||'");
                query = $@"SELECT 'INSERT INTO ""{tableIndent}"" VALUES({string.Join(",", columnNames)})' FROM ""{tableIndent}"";";
                r = Query(new[] { query }, false, false);
                foreach (var x in r[0].Values)
                {
                    w.Write($"{x[0]};\n");
                }
            }
            // Do indexes, triggers, and views.
            query = @"SELECT ""name"", ""type"", ""sql"" FROM ""sqlite_master""
              WHERE ""sql"" NOT NULL AND ""type"" IN('index', 'trigger', 'view')";
            rows = Query(new[] { query }, false, false);
            foreach (var v in rows[0].Values)
            {
                w.Write($"{v[2]};\n");
            }
            w.Write("COMMIT;\n");
        }

        /// <summary>
        /// Close closes the connection.
        /// </summary>
        public void Close()
        {
            sqlite.Close();
        }

        private static void copyDatabase(SQLiteConnection dst, SQLiteConnection src)
        {
            src.BackupDatabase(dst, "main", "main", -1, null, bkDelay);
        }


        /// <summary>
        /// normalizeRowValues performs some normalization of values in the returned rows.
        /// Text values come over (from sqlite-go) as []byte instead of strings
        /// for some reason, so we have explicitly convert (but only when type
        /// is "text" so we don't affect BLOB types)
        /// </summary>
        /// <param name="row"></param>
        /// <param name="types"></param>
        /// <returns></returns>
        private object[] normalizeRowValues(object[] row, string[] types)
        {
            var values = new object[types.Length];
            for (int i = 0; i < row.Length; i++)
            {
                if (isTextType(types[i]))
                {
                    if (row[i] is byte[])
                    {
                        values[i] = Encoding.UTF8.GetString(row[i] as byte[]);
                    }
                    else
                    {
                        values[i] = row[i];
                    }
                }
                else
                {
                    values[i] = row[i];
                }
            }
            return values;
        }

        /// <summary>
        /// isTextType returns whether the given type has a SQLite text affinity.
        /// http://www.sqlite.org/datatype3.html
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        private bool isTextType(string t)
        {
            return t == "text" ||
                    t == "json" ||
                    t == "" ||
                    t.StartsWith("varchar") ||
                    t.StartsWith("varying character") ||
                    t.StartsWith("nchar") ||
                    t.StartsWith("native character") ||
                    t.StartsWith("nvarchar") ||
                    t.StartsWith("clob");
        }
    }
}