using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Rafty.Infrastructure;
using Rafty.Log;

namespace RQLite.Net.Raft
{
    public class SQLiteLog : ILog
    {
        private readonly string _path;
        private readonly SemaphoreSlim _sempaphore = new SemaphoreSlim(1, 1);
        private readonly ILogger _logger;
        private readonly NodeId _nodeId;
        private JsonSerializerSettings _settings;

        public SQLiteLog(NodeId nodeId, ILoggerFactory loggerFactory) : this($"{nodeId.Id.Replace("/", "").Replace(":", "")}.log.db", nodeId, loggerFactory)
        { }

        public SQLiteLog(string sqliteDbPath, NodeId nodeId, ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<SQLiteLog>();
            _nodeId = nodeId;
            _path = $"{nodeId.Id.Replace("/", "").Replace(":", "")}.log.db";
            _settings = new JsonSerializerSettings()
            {
                TypeNameHandling = TypeNameHandling.All
            };

            _sempaphore.Wait();

            if (!File.Exists(_path))
            {
                var fs = File.Create(_path);

                fs.Dispose();

                using (var connection = new SQLiteConnection($"Data Source={_path};"))
                {
                    connection.Open();

                    const string sql = @"create table logs (
                        id integer primary key,
                        data text not null
                    )";

                    using (var command = new SQLiteCommand(sql, connection))
                    {
                        var result = command.ExecuteNonQuery();

                        _logger.LogInformation(result == 0
                            ? $"id: {_nodeId.Id} create database, result: {result}"
                            : $"id: {_nodeId.Id} did not create database., result: {result}");
                    }
                }
            }

            _sempaphore.Release();
        }

        public async Task<int> LastLogIndex()
        {
            _sempaphore.Wait();
            var result = 1;
            using (var connection = new SQLiteConnection($"Data Source={_path};"))
            {
                connection.Open();
                var sql = @"select id from logs order by id desc limit 1";
                using (var command = new SQLiteCommand(sql, connection))
                {
                    var index = Convert.ToInt32(await command.ExecuteScalarAsync());
                    if (index > result)
                    {
                        result = index;
                    }
                }
            }

            _sempaphore.Release();
            return result;
        }

        public async Task<long> LastLogTerm()
        {
            _sempaphore.Wait();
            long result = 0;
            using (var connection = new SQLiteConnection($"Data Source={_path};"))
            {
                connection.Open();
                var sql = @"select data from logs order by id desc limit 1";
                using (var command = new SQLiteCommand(sql, connection))
                {
                    var data = Convert.ToString(await command.ExecuteScalarAsync());
                    var log = JsonConvert.DeserializeObject<LogEntry>(data, _settings);
                    if (log != null && log.Term > result)
                    {
                        result = log.Term;
                    }
                }
            }
            _sempaphore.Release();
            return result;
        }

        public async Task<int> Count()
        {
            _sempaphore.Wait();
            var result = 0;
            using (var connection = new SQLiteConnection($"Data Source={_path};"))
            {
                connection.Open();
                var sql = @"select count(id) from logs";
                using (var command = new SQLiteCommand(sql, connection))
                {
                    var index = Convert.ToInt32(await command.ExecuteScalarAsync());
                    if (index > result)
                    {
                        result = index;
                    }
                }
            }
            _sempaphore.Release();
            return result;
        }

        public async Task<int> Apply(LogEntry log)
        {
            _sempaphore.Wait();
            using (var connection = new SQLiteConnection($"Data Source={_path};"))
            {
                connection.Open();
                var data = JsonConvert.SerializeObject(log, _settings);
                //todo - sql injection dont copy this..
                var sql = $"insert into logs (data) values ('{data}')";
                _logger.LogInformation($"id: {_nodeId.Id}, sql: {sql}");
                using (var command = new SQLiteCommand(sql, connection))
                {
                    var result = await command.ExecuteNonQueryAsync();
                    _logger.LogInformation($"id: {_nodeId.Id}, insert log result: {result}");
                }

                sql = "select last_insert_rowid()";
                using (var command = new SQLiteCommand(sql, connection))
                {
                    var result = await command.ExecuteScalarAsync();
                    _logger.LogInformation($"id: {_nodeId.Id}, about to release semaphore");
                    _sempaphore.Release();
                    _logger.LogInformation($"id: {_nodeId.Id}, saved log to sqlite");
                    return Convert.ToInt32(result);
                }
            }
        }

        public async Task DeleteConflictsFromThisLog(int index, LogEntry logEntry)
        {
            _sempaphore.Wait();
            using (var connection = new SQLiteConnection($"Data Source={_path};"))
            {
                connection.Open();
                //todo - sql injection dont copy this..
                var sql = $"select data from logs where id = {index};";
                _logger.LogInformation($"id: {_nodeId.Id} sql: {sql}");
                using (var command = new SQLiteCommand(sql, connection))
                {
                    var data = Convert.ToString(await command.ExecuteScalarAsync());

                    _logger.LogInformation($"id {_nodeId.Id} got log for index: {index}, data is {data} and new log term is {logEntry.Term}");

                    var log = JsonConvert.DeserializeObject<LogEntry>(data, _settings);
                    if (logEntry != null && log != null && logEntry.Term != log.Term)
                    {
                        //todo - sql injection dont copy this..
                        var deleteSql = $"delete from logs where id >= {index};";
                        _logger.LogInformation($"id: {_nodeId.Id} sql: {deleteSql}");
                        using (var deleteCommand = new SQLiteCommand(deleteSql, connection))
                        {
                            var result = await deleteCommand.ExecuteNonQueryAsync();
                        }
                    }
                }
            }
            _sempaphore.Release();
        }

        public async Task<bool> IsDuplicate(int index, LogEntry logEntry)
        {
            _sempaphore.Wait();
            using (var connection = new SQLiteConnection($"Data Source={_path};"))
            {
                connection.Open();
                //todo - sql injection dont copy this..
                var sql = $"select data from logs where id = {index};";
                using (var command = new SQLiteCommand(sql, connection))
                {
                    var data = Convert.ToString(await command.ExecuteScalarAsync());

                    var log = JsonConvert.DeserializeObject<LogEntry>(data, _settings);

                    if (logEntry != null && log != null && logEntry.Term == log.Term)
                    {
                        _sempaphore.Release();
                        return true;
                    }
                }
            }

            _sempaphore.Release();
            return false;
        }

        public async Task<LogEntry> Get(int index)
        {
            _sempaphore.Wait();
            using (var connection = new SQLiteConnection($"Data Source={_path};"))
            {
                connection.Open();
                //todo - sql injection dont copy this..
                var sql = $"select data from logs where id = {index}";
                using (var command = new SQLiteCommand(sql, connection))
                {
                    var data = Convert.ToString(await command.ExecuteScalarAsync());
                    var log = JsonConvert.DeserializeObject<LogEntry>(data, _settings);
                    _sempaphore.Release();
                    return log;
                }
            }
        }

        public async Task<List<(int index, LogEntry logEntry)>> GetFrom(int index)
        {
            _sempaphore.Wait();
            var logsToReturn = new List<(int, LogEntry)>();

            using (var connection = new SQLiteConnection($"Data Source={_path};"))
            {
                connection.Open();
                //todo - sql injection dont copy this..
                var sql = $"select id, data from logs where id >= {index}";
                using (var command = new SQLiteCommand(sql, connection))
                {
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (reader.Read())
                        {
                            var id = Convert.ToInt32(reader[0]);
                            var data = (string)reader[1];
                            var log = JsonConvert.DeserializeObject<LogEntry>(data, _settings);
                            logsToReturn.Add((id, log));
                        }
                    }
                }
                _sempaphore.Release();
                return logsToReturn;
            }
        }

        public async Task<long> GetTermAtIndex(int index)
        {
            _sempaphore.Wait();
            long result = 0;
            using (var connection = new SQLiteConnection($"Data Source={_path};"))
            {
                connection.Open();
                //todo - sql injection dont copy this..
                var sql = $"select data from logs where id = {index}";
                using (var command = new SQLiteCommand(sql, connection))
                {
                    var data = Convert.ToString(await command.ExecuteScalarAsync());

                    var log = JsonConvert.DeserializeObject<LogEntry>(data, _settings);
                    if (log != null && log.Term > result)
                    {
                        result = log.Term;
                    }
                }
            }
            _sempaphore.Release();
            return result;
        }
        public async Task Remove(int indexOfCommand)
        {
            _sempaphore.Wait();
            using (var connection = new SQLiteConnection($"Data Source={_path};"))
            {
                connection.Open();
                //todo - sql injection dont copy this..
                var deleteSql = $"delete from logs where id >= {indexOfCommand};";
                _logger.LogInformation($"id: {_nodeId.Id} Remove {deleteSql}");
                using (var deleteCommand = new SQLiteCommand(deleteSql, connection))
                {
                    var result = await deleteCommand.ExecuteNonQueryAsync();
                }
            }
            _sempaphore.Release();
        }

    }
}