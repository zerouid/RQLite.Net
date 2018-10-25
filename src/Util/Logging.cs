using Microsoft.Extensions.Logging;

namespace RQLite.Sharp.Util
{
    public class Logging
    {
        public static ILoggerFactory LoggerFactory { get; } = new LoggerFactory();
    }
}