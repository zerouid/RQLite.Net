using System;
using System.Net;
using System.Net.Http;
using System.Text;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using RQLite.Net.Util;

namespace RQLite.Net.Disco
{
    public class DiscoveryClient
    {
        private string baseUrl;
        private ILogger logger;

        /// <summary>
        /// New returns an initialized Discovery Service client.
        /// </summary>
        /// <param name="url"></param>
        public DiscoveryClient(string url)
        {
            baseUrl = url;
            logger = Logging.LoggerFactory.CreateLogger("[discovery]");
        }

        public string URL { get { return baseUrl; } }

        public DiscoveryResponse Register(string id, string addr)
        {
            var m = new { addr };
            var url = registrationURL(baseUrl, id);
            using (var client = new HttpClient())
            {
                logger.LogInformation($"discovery client attempting registration of {addr} at {url}");
                var content = new StringContent(JsonConvert.SerializeObject(m), Encoding.UTF8, "application/json");
                var resp = client.PostAsync(url, content).Result;
                var b = resp.Content.ReadAsStringAsync().Result;
                switch (resp.StatusCode)
                {
                    case HttpStatusCode.OK:
                        var r = JsonConvert.DeserializeObject<DiscoveryResponse>(b);
                        logger.LogInformation($"discovery client successfully registered {addr} at {url}");
                        return r;
                    default:
                        throw new Exception(resp.ReasonPhrase);
                }
            }
        }

        private static string registrationURL(string url, string id)
        {
            return $"{url}/{id}";
        }
    }
}