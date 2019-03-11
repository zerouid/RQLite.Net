using Xunit;
using RQLite.Net.Auth;
using System.IO;

namespace RQLite.Net.Tests.Auth
{
    public class CredentialStoreTest
    {
        private class TestBasicAuther : IBasicAuther
        {
            public string Username { get; set; }
            public string Password { get; set; }
            public bool Ok { get; set; }

            public (string username, string password, bool ok) BasicAuth()
            {
                return (Username, Password, Ok);
            }
        };

        [Fact]
        public void AuthLoadSingleTest()
        {
            const string jsonStream = @"
                [
                    {""username"":""username1"", ""password"":""password1""}
                ]
            ";
            var store = new CredentialStore();

            store.Load(new StringReader(jsonStream));

            bool check = store.Check("username1", "password1");
            Assert.True(check, "single credential not loaded correctly");

            check = store.Check("username1", "wrong");
            Assert.False(check, "single credential not loaded correctly");

            check = store.Check("wrong", "password1");
            Assert.False(check, "single credential not loaded correctly");

            check = store.Check("wrong", "wrong");
            Assert.False(check, "single credential not loaded correctly");
        }

        [Fact]
        public void AuthLoadMultipleTest()
        {
            const string jsonStream = @"
                [
                    {""username"":""username1"", ""password"":""password1""},
                    {""username"":""username2"", ""password"":""password2""}
                ]
            ";
            var store = new CredentialStore();

            store.Load(new StringReader(jsonStream));

            bool check = store.Check("username1", "password1");
            Assert.True(check, "username1 credential not loaded correctly");

            check = store.Check("username1", "password2");
            Assert.False(check, "username1 credential not loaded correctly");

            check = store.Check("username2", "password2");
            Assert.True(check, "username2 credential not loaded correctly");

            check = store.Check("username2", "password1");
            Assert.False(check, "username2 credential not loaded correctly");

            check = store.Check("username1", "wrong");
            Assert.False(check, "multiple credential not loaded correctly");

            check = store.Check("wrong", "password1");
            Assert.False(check, "multiple credential not loaded correctly");

            check = store.Check("wrong", "wrong");
            Assert.False(check, "multiple credential not loaded correctly");
        }

        [Fact]
        public void AuthLoadSingleRequestTest()
        {
            const string jsonStream = @"
                [
                    {""username"":""username1"", ""password"":""password1""}
                ]
            ";
            var store = new CredentialStore();

            store.Load(new StringReader(jsonStream));

            var b1 = new TestBasicAuther { Username = "username1", Password = "password1", Ok = true };
            var b2 = new TestBasicAuther { Username = "username1", Password = "wrong", Ok = true };
            var b3 = new TestBasicAuther();

            var check = store.CheckRequest(b1);
            Assert.True(check, "username1 (b1) credential not checked correctly via request");

            check = store.CheckRequest(b2);
            Assert.False(check, "username1 (b2) credential not checked correctly via request");

            check = store.CheckRequest(b3);
            Assert.False(check, "username1 (b3) credential not checked correctly via request");
        }

        [Fact]
        public void AuthPermsLoadSingleTest()
        {
            const string jsonStream = @"
                [
                    {
                        ""username"": ""username1"",
                        ""password"": ""password1"",
                        ""perms"": [""foo"", ""bar""]
                    },
                    {
                        ""username"": ""username2"",
                        ""password"": ""password1"",
                        ""perms"": [""baz""]
                    }
                ]
            ";
            var store = new CredentialStore();

            store.Load(new StringReader(jsonStream));

            var check = store.Check("username1", "password1");
            Assert.True(check, "single credential not loaded correctly");

            check = store.Check("username1", "wrong");
            Assert.False(check, "single credential not loaded correctly");

            var perm = store.HasPerm("wrong", "foo");
            Assert.False(perm, "wrong has foo perm");

            perm = store.HasPerm("username1", "foo");
            Assert.True(perm, "username1 does not have foo perm");

            perm = store.HasPerm("username1", "bar");
            Assert.True(perm, "username1 does not have bar perm");

            perm = store.HasPerm("username1", "baz");
            Assert.False(perm, "username1 does have baz perm");

            perm = store.HasPerm("username2", "baz");
            Assert.True(perm, "username2 does not have baz perm");


            perm = store.HasAnyPerm("username1", "foo");
            Assert.True(perm, "username1 does not have foo perm");

            perm = store.HasAnyPerm("username1", "bar");
            Assert.True(perm, "username1 does not have bar perm");

            perm = store.HasAnyPerm("username1", "foo", "bar");
            Assert.True(perm, "username1 does not have foo or bar perm");

            perm = store.HasAnyPerm("username1", "foo", "qux");
            Assert.True(perm, "username1 does not have foo or qux perm");

            perm = store.HasAnyPerm("username1", "qux", "bar");
            Assert.True(perm, "username1 does not have bar perm");

            perm = store.HasAnyPerm("username1", "baz", "qux");
            Assert.False(perm, "username1 has baz or qux perm");
        }

        [Fact]
        public void AuthLoadHashedSingleRequestTest()
        {
            const string jsonStream = @"
                [
                    {
                        ""username"": ""username1"",
                        ""password"": ""$2a$10$fKRHxrEuyDTP6tXIiDycr.nyC8Q7UMIfc31YMyXHDLgRDyhLK3VFS""
                    },
                    {
                        ""username"": ""username2"",
                        ""password"": ""password2""
                    }
                ]
            ";
            var store = new CredentialStore();

            store.Load(new StringReader(jsonStream));

            var b1 = new TestBasicAuther { Username = "username1", Password = "password1", Ok = true };
            var b2 = new TestBasicAuther { Username = "username2", Password = "password2", Ok = true };
            var b3 = new TestBasicAuther { Username = "username1", Password = "wrong", Ok = true };
            var b4 = new TestBasicAuther { Username = "username2", Password = "wrong", Ok = true };

            var check = store.CheckRequest(b1);
            Assert.True(check, "username1 (b1) credential not checked correctly via request");

            check = store.CheckRequest(b2);
            Assert.True(check, "username2 (b2) credential not checked correctly via request");

            check = store.CheckRequest(b3);
            Assert.False(check, "username1 (b3) credential not checked correctly via request");

            check = store.CheckRequest(b4);
            Assert.False(check, "username2 (b4) credential not checked correctly via request");
        }

        [Fact]
        public void AuthPermsRequestLoadSingleTest()
        {
            const string jsonStream = @"
                [
                    {
                        ""username"": ""username1"",
                        ""password"": ""password1"",
                        ""perms"": [""foo"", ""bar""]
                    }
                ]
            ";
            var store = new CredentialStore();

            store.Load(new StringReader(jsonStream));

            var check = store.Check("username1", "password1");
            Assert.True(check, "single credential not loaded correctly");

            var b1 = new TestBasicAuther { Username = "username1", Password = "password1", Ok = true };
            var perm = store.HasPermRequest(b1, "foo");
            Assert.True(perm, "username1 does not has perm foo via request");

            var b2 = new TestBasicAuther { Username = "username2", Password = "password1", Ok = true };
            perm = store.HasPermRequest(b2, "foo");
            Assert.False(perm, "username2 does have perm foo via request");
        }

        [Fact]
        public void AuthPermsEmptyLoadSingleTest()
        {
            const string jsonStream = @"
                [
                    {
                        ""username"": ""username1"",
                        ""password"": ""password1"",
                        ""perms"": []
                    }
                ]
            ";
            var store = new CredentialStore();

            store.Load(new StringReader(jsonStream));

            var check = store.Check("username1", "password1");
            Assert.True(check, "single credential not loaded correctly");

            check = store.Check("username1", "wrong");
            Assert.False(check, "single credential not loaded correctly");

            var perm = store.HasPerm("username1", "foo");
            Assert.False(perm, "username1 has foo perm");
        }

        [Fact]
        public void AuthPermsNullLoadSingleTest()
        {
            const string jsonStream = @"
                [
                    {
                        ""username"": ""username1"",
                        ""password"": ""password1""
                    }
                ]
            ";
            var store = new CredentialStore();

            store.Load(new StringReader(jsonStream));

            var check = store.Check("username1", "password1");
            Assert.True(check, "single credential not loaded correctly");

            check = store.Check("username1", "wrong");
            Assert.False(check, "single credential not loaded correctly");

            var perm = store.HasPerm("username1", "foo");
            Assert.False(perm, "username1 has foo perm");
        }
    }
}