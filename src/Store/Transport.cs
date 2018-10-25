using System;
using System.Net.Sockets;

namespace RQLite.Sharp.Store
{
    /// <summary>
    /// Transport is the network service provided to Raft, and wraps a Listener.
    /// </summary>
    public class Transport
    {
        private IListener ln;
        /// <summary>
        /// NewTransport returns an initialized Transport.
        /// </summary>
        /// <param name="ln"></param>
        public Transport(IListener ln)
        {
            this.ln = ln;
        }

        /// <summary>
        /// Dial creates a new network connection.
        /// </summary>
        /// <param name="addr"></param>
        /// <param name="timeout"></param>
        /// <returns></returns>
        public Socket Dial(string addr, TimeSpan timeout)
        {
            return ln.Dial(addr, timeout);
        }

        /// <summary>
        /// Accept waits for the next connection.
        /// </summary>
        /// <returns></returns>
        public Socket Accept()
        {
            return ln.Accept();
        }

        /// <summary>
        /// Close closes the transport
        /// </summary>
        public void Close()
        {
            ln.Close();
        }

        /// <summary>
        /// Addr returns the binding address of the transport.
        /// </summary>
        /// <value></value>
        public string Addr { get { return ln.Addr; } }
    }
}