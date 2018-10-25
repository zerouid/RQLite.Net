
using System;
using System.Collections.Generic;
using RQLite.Sharp.Util;
using Microsoft.Extensions.Logging;
using System.Threading;
using System.Net;
using System.Net.Http;
using System.Runtime.Serialization.Json;
using Newtonsoft.Json;
using System.Text;

namespace RQLite.Sharp.Cluster
{
    /// <summary>
    /// Package cluster supports intracluster control messaging.
    /// </summary>
    public static class Cluster
    {
        const int numAttempts = 3;
        private static readonly TimeSpan attemptInterval = TimeSpan.FromSeconds(5);
        public static string Join(IEnumerable<string> joinAddr, string id, string addr, IDictionary<string, string> meta, bool skip)
        {
            var logger = Logging.LoggerFactory.CreateLogger("[cluster-join]");
            Exception err = null;
            for (int i = 0; i < numAttempts; i++)
            {
                foreach (var a in joinAddr)
                {
                    try
                    {
                        return AttemptJoin(a, id, addr, meta, skip);
                    }
                    catch (Exception ex)
                    {
                        err = ex;
                        logger.LogDebug(ex, $"Attemp to join cluster at {a} failed.");
                    }
                }
                logger.LogError($"failed to join cluster at [{string.Join(", ", joinAddr)}], sleeping {attemptInterval} before retry.");
                Thread.Sleep(attemptInterval);
            }
            logger.LogError($"failed to join cluster at [{string.Join(", ", joinAddr)}], after {numAttempts} attempts");
            throw err ?? new Exception("Unknown error.");
        }

        private static string AttemptJoin(string joinAddr, string id, string addr, IDictionary<string, string> meta, bool skip)
        {
            if (string.IsNullOrEmpty(id))
            {
                throw new ArgumentException("node ID not set");
            }
            var resv = IPAddress.Parse(addr);
            var fullAddr = new UriBuilder($"{joinAddr}/join").Uri;

            ServicePointManager.SecurityProtocol = SecurityProtocolType.Tls | SecurityProtocolType.Tls11 | SecurityProtocolType.Tls12;
            if (skip)
            {
                ServicePointManager.ServerCertificateValidationCallback = (sender, cert, chain, sslPolicyErrors) => true;
            }

            using (var client = new HttpClient())
            {
                var b = JsonConvert.SerializeObject(new { id, meta, addr = resv.ToString() });
                var content = new StringContent(b, Encoding.UTF8, "application/json");
                var resp = client.PostAsync(fullAddr, content).Result;
                switch (resp.StatusCode)
                {
                    case HttpStatusCode.OK:
                        return fullAddr.ToString();
                    default:
                        throw new Exception($"failed to join, node returned: {resp.ReasonPhrase}: ({resp.Content.ReadAsStringAsync().Result})");
                }
            }
        }
    }
}