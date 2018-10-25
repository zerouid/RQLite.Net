using System.Collections.Generic;

namespace RQLite.Sharp.Auth
{
    /// <summary>
    /// Credential represents authentication and authorization configuration for a single user.
    /// </summary>
    public class Credential
    {
        public string Username { get; set; }
        public string Password { get; set; }
        public IEnumerable<string> Perms { get; set; }
    }
}