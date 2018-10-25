using System.Threading.Tasks;
using Rafty.Concensus.Messages;
using Rafty.Concensus.Peers;
using Rafty.FiniteStateMachine;
using Rafty.Infrastructure;

namespace RQLite.Net.Raft
{
    public class HttpPeer : IPeer
    {
        public string Id => throw new System.NotImplementedException();

        public Task<RequestVoteResponse> Request(RequestVote requestVote)
        {
            throw new System.NotImplementedException();
        }

        public Task<AppendEntriesResponse> Request(AppendEntries appendEntries)
        {
            throw new System.NotImplementedException();
        }

        public Task<Response<T>> Request<T>(T command) where T : ICommand
        {
            throw new System.NotImplementedException();
        }
    }
}