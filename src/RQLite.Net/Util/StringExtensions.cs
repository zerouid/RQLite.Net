using System.Net;
using System;
using System.Globalization;

namespace RQLite.Net.Util
{
    public static class StringExtensions
    {
        public static IPEndPoint ToIPEndPoint(this string addr)
        {
            if (string.IsNullOrEmpty(addr))
            {
                throw new ArgumentNullException(nameof(addr));
            }
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