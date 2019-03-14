using Xunit;
using RQLite.Net.Cluster;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using System.Net;
using Microsoft.AspNetCore.TestHost;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Builder;
using Moq;
using Moq.Protected;
using System.Linq;
using System;

namespace RQLite.Net.Tests.Cluster
{
    public class ClusterTest
    {
        [Fact]
        public void SingleJoinOKTest()
        {
            var serverTestAddress = "http://srv1";
            var factoryMock = SetupHttpClientMocks(HttpMethod.Post, "/join", (serverTestAddress, HttpStatusCode.OK));
            var cluster = new RQLite.Net.Cluster.Cluster(factoryMock);

            var j = cluster.Join(new[] { serverTestAddress }, "id0", "127.0.0.1:9090", null, true);
            Assert.Equal(serverTestAddress + "/join", j);
        }

        [Fact]
        public void SingleJoinMetaOKTest()
        {

            var serverTestAddress = "http://srv1";
            var factoryMock = SetupHttpClientMocks(HttpMethod.Post, "/join", (serverTestAddress, HttpStatusCode.OK));
            var cluster = new RQLite.Net.Cluster.Cluster(factoryMock);

            var j = cluster.Join(new[] { serverTestAddress }, "id0", "127.0.0.1:9090", null, true);
            Assert.Equal(serverTestAddress + "/join", j);
        }

        [Fact]
        public void SingleJoinFailTest()
        {

        }

        [Fact]
        public void DoubleJoinOKTest()
        {

        }

        [Fact]
        public void DoubleJoinOKSecondNodetest()
        {

        }

        [Fact]
        public void DoubleJoinOKSecondNodeRedirectTest()
        {

        }

        private IHttpClientFactory SetupHttpClientMocks(HttpMethod method, string path, params (string addr, HttpStatusCode status)[] endpoints)
        {
            var httpHandlerMock = new Mock<HttpMessageHandler>();
            httpHandlerMock
                .Protected()
                .Setup<Task<HttpResponseMessage>>(
                    "SendAsync",
                    ItExpr.IsAny<HttpRequestMessage>(),
                    ItExpr.IsAny<CancellationToken>()
                )
                .ReturnsAsync((HttpRequestMessage r, CancellationToken ct) =>
                    {
                        if (r.Method == method && r.RequestUri.AbsolutePath == path)
                        {
                            var endpoint = endpoints.FirstOrDefault(x => x.addr == r.RequestUri.GetLeftPart(UriPartial.Authority));
                            if (endpoint != default)
                            {
                                return new HttpResponseMessage { StatusCode = endpoint.status };
                            }

                        }
                        return new HttpResponseMessage { StatusCode = HttpStatusCode.BadRequest };
                    }
                )
                .Verifiable();

            var httpClientFactoryMock = new Mock<IHttpClientFactory>(MockBehavior.Strict);
            httpClientFactoryMock
                .Setup(f => f.CreateClient(It.IsAny<string>()))
                .Returns(new HttpClient(httpHandlerMock.Object));

            return httpClientFactoryMock.Object;
        }
    }
}