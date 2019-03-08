using System.Collections.Generic;
using Rafty.Infrastructure;
using RQLite.Net.Db;

namespace RQLite.Net.Store
{
    public class FsmQueryResponse<T> : OkResponse<T>
    {
        public FsmQueryResponse(T command) : base(command)
        {
        }
        public IEnumerable<Rows> Rows { get; set; }
    }
    public class FsmExecuteResponse<T> : OkResponse<T>
    {
        public FsmExecuteResponse(T command) : base(command)
        {
        }

        public IEnumerable<Result> Results { get; set; }
    }
}