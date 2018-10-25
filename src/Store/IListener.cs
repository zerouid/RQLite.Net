using System;
using System.Net.Sockets;

namespace RQLite.Sharp.Store
{
    /// <summary>
    /// Listener is the interface Raft-compatible network layers
    /// should implement.
    /// </summary>
    public interface IListener
    {
        Socket Dial(string addr, TimeSpan timeout);
        Socket Accept();
        void Close();
        string Addr { get; }
    }
}