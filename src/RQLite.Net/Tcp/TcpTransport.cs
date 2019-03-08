using System;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using RQLite.Net.Store;

namespace RQLite.Net.Tcp
{
    /// <summary>
    /// Transport is the network layer for internode communications.
    /// </summary>
    public class TcpTransport
    {
        private TcpListener ln;
        // Path to local X.509 cert.
        private string certFile;
        // Path to corresponding X.509 key.
        private string certKey;
        // Remote nodes use encrypted communication.
        private bool remoteEncrypted;
        // Skip verification of remote node certs.
        private bool skipVerify;

        private X509Certificate2 serverCertificate;

        /// <summary>
        /// NewTransport returns an initialized unecrypted Transport.
        /// </summary>
        public TcpTransport() { }

        /// <summary>
        /// NewTLSTransport returns an initialized TLS-ecrypted Transport.
        /// </summary>
        /// <param name="certFile"></param>
        /// <param name="keyPath"></param>
        /// <param name="skipVerify"></param>
        public TcpTransport(string certFile, string keyPath, bool skipVerify)
        {
            this.certFile = certFile;
            this.certKey = keyPath;
            this.skipVerify = skipVerify;
            remoteEncrypted = true;
        }

        /// <summary>
        /// Open opens the transport, binding to the supplied address.
        /// </summary>
        /// <param name="addr"></param>
        public void Open(string addr)
        {
            ln = new TcpListener(parseAddr(addr));
            if (!string.IsNullOrEmpty(certFile))
            {
                serverCertificate = new X509Certificate2();
                serverCertificate.Import(certFile);
                serverCertificate.PrivateKey = loadPrivateKey(certKey)
            }
        }


private static AsymmetricAlgorithm loadPrivateKey(string keyFile)
{
    
}
        private static IPEndPoint parseAddr(string addr)
        {
            int addressLength = addr.Length;  // If there's no port then send the entire string to the address parser
            int lastColonPos = addr.LastIndexOf(':');

            // Look to see if this is an IPv6 address with a port.
            if (lastColonPos > 0)
            {
                if (addr[lastColonPos - 1] == ']')
                {
                    addressLength = lastColonPos;
                }
                // Look to see if this is IPv4 with a port (IPv6 will have another colon)
                else if (addr.Substring(0, lastColonPos).LastIndexOf(':') == -1)
                {
                    addressLength = lastColonPos;
                }
            }

            if (IPAddress.TryParse(addr.Substring(0, addressLength), out IPAddress address))
            {
                uint port = 0;
                if (addressLength == addr.Length ||
                    (uint.TryParse(addr.Substring(addressLength + 1), NumberStyles.None, CultureInfo.InvariantCulture, out port) && port <= IPEndPoint.MaxPort))

                {
                    return new IPEndPoint(address, (int)port);
                }
            }

            throw new ArgumentException("Invalid endpoint descriptor.");
        }

    }
}