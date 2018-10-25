using System;
using System.Collections.Generic;
using System.Data.SQLite;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Rafty.FiniteStateMachine;
using Rafty.Infrastructure;
using Rafty.Log;

namespace RQLite.Net.Raft
{
    public class SQLiteFSM : IFiniteStateMachine
    {
        private readonly string _path;
        private readonly SemaphoreSlim _sempaphore = new SemaphoreSlim(1, 1);
        private readonly ILogger _logger;
        private readonly NodeId _nodeId;
        private JsonSerializerSettings _settings;

        public SQLiteFSM(NodeId nodeId, ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<SQLiteLog>();
            _nodeId = nodeId;
            _path = $"{nodeId.Id.Replace("/", "").Replace(":", "")}.state.db";
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

                    const string sql = @"create table state (
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

        public async Task Handle(LogEntry log)
        {
            try
            {
                _logger.LogInformation($"id: {_nodeId.Id} applying log to state machine log.Term: {log.Term}");
                await _sempaphore.WaitAsync();

                using (var connection = new SQLiteConnection($"Data Source={_path};"))
                {
                    connection.Open();

                    var next = JsonConvert.SerializeObject(log.CommandData, _settings);

                    var sql = $"insert into state (data) values (@data)";
                    using (var command = new SQLiteCommand(sql, connection))
                    {
                        command.Parameters.AddWithValue("@data", next);
                        var current = await command.ExecuteNonQueryAsync();
                    }
                }
            }
            catch (Exception e)
            {
                _logger.LogInformation($"id: {_nodeId.Id} threw an exception when trying to handle log entry, exception: {e}");
                throw;
            }
            finally
            {
                _sempaphore.Release();
            }
        }
    }
}