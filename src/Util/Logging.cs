using Microsoft.Extensions.Logging;

namespace RQLite.Net.Util
{
    public class Logging
    {
        public static ILoggerFactory LoggerFactory { get; } = new LoggerFactory();
    }
}