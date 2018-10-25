using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json;

/// <summary>
/// Package auth is a lightweight credential store.
/// It provides functionality for loading credentials, as well as validating credentials.
/// </summary>
namespace RQLite.Sharp.Auth
{
    /// <summary>
    /// CredentialsStore stores authentication and authorization information for all users.
    /// </summary>
    public class CredentialsStore
    {
        private IDictionary<string, string> store;
        private IDictionary<string, IDictionary<string, bool>> perms;
        // NewCredentialsStore returns a new instance of a CredentialStore.
        public CredentialsStore()
        {
            store = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
            perms = new Dictionary<string, IDictionary<string, bool>>(StringComparer.OrdinalIgnoreCase);
        }
        /// <summary>
        /// Load loads credential information from a reader.
        /// </summary>
        public void Load(TextReader rdr)
        {
            using (var jsonRdr = new JsonTextReader(rdr))
            {
                var serializer = new JsonSerializer();
                foreach (var cred in serializer.Deserialize<IEnumerable<Credential>>(jsonRdr))
                {
                    store.Add(cred.Username, cred.Password);
                    perms.Add(cred.Username, cred.Perms.ToDictionary(_ => _, _ => true, StringComparer.OrdinalIgnoreCase));
                }
            }
        }

        /// <summary>
        /// Check returns true if the password is correct for the given username.
        /// </summary>
        /// <param name="username"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        public bool Check(string username, string password)
        {
            if (!store.TryGetValue(username, out string pw))
            {
                return false;
            }

            return password == pw || BCrypt.Net.BCrypt.Verify(password, pw);
        }
        /// <summary>
        /// CheckRequest returns true if b contains a valid username and password.
        /// </summary>
        /// <param name="b"></param>
        /// <returns></returns>
        public bool CheckRequest(IBasicAuther b)
        {
            return b != null && Check(b.Username, b.Password);
        }
        /// <summary>
        /// HasPerm returns true if username has the given perm. It does not perform any password checking.
        /// </summary>
        /// <param name="username"></param>
        /// <param name="perm"></param>
        /// <returns></returns>
        public bool HasPerm(string username, string perm)
        {
            if (!perms.TryGetValue(username, out IDictionary<string, bool> m))
            {
                return false;
            }

            return m.TryGetValue(perm, out bool p) && p;
        }
        /// <summary>
        /// HasAnyPerm returns true if username has at least one of the given perms. It does not perform any password checking.
        /// </summary>
        /// <param name="username"></param>
        /// <param name="perm"></param>
        /// <returns></returns>
        public bool HasAnyPerm(string username, params string[] perm)
        {
            return perm != null && perm.Any(p => HasPerm(username, p));
        }
        /// <summary>
        /// HasPermRequest returns true if the username returned by b has the givem perm.
        /// It does not perform any password checking, but if there is no username in the request, it returns false.
        /// </summary>
        /// <param name="basic"></param>
        /// <param name="perm"></param>
        /// <returns></returns>
        public bool HasPermRequest(IBasicAuther b, string perm)
        {
            return b != null && HasPerm(b.Username, perm);
        }
    }
}