using System;
using System.Net;
using System.Net.Http;

/// <summary>
/// Package aws provides functionality for accessing the AWS API.
/// </summary>
namespace RQLite.Net.AWS
{
    /// <summary>
    /// MetadataClient is a client for fetching AWS EC2 instance metadata.
    /// </summary>
    public class MetadataClient
    {
        private string URL;
        /// <summary>
        /// NewMetadataClient returns an instance of a MetadataClient.
        /// </summary>
        public MetadataClient()
        {
            URL = "http://169.254.169.254/";
        }
        /// <summary>
        /// LocalIPv4 returns the private IPv4 address of the instance.
        /// </summary>
        /// <returns></returns>
        public string LocalIPv4()
        {
            return get("/latest/meta-data/local-ipv4");
        }
        /// <summary>
        /// PublicIPv4 returns the public IPv4 address of the instance.
        /// </summary>
        /// <returns></returns>
        public string PublicIPv4()
        {
            return get("/latest/meta-data/public-ipv4");
        }

        private string get(string path)
        {
            using (var client = new HttpClient())
            {
                var resp = client.GetAsync(path).Result;
                if (resp.StatusCode != HttpStatusCode.OK)
                {
                    throw new Exception($"Failed to request {path}, got: {resp.ReasonPhrase}");
                }
                return resp.Content.ReadAsStringAsync().Result;
            }
        }
    }
}