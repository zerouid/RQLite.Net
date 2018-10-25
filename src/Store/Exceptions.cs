using System;
using Rafty.Infrastructure;

namespace RQLite.Sharp.Store
{
    public class RaftException<T> : Exception
    {
        public RaftException(ErrorResponse<T> response) : base(response.Error) { }
    }
    /// <summary>
    /// ErrNotLeader is returned when a node attempts to execute a leader-only
    /// operation.
    /// </summary>
    public class NotLeaderException : Exception
    {
        public NotLeaderException() : base("not leader") { }
    }
    /// <summary>
    /// ErrOpenTimeout is returned when the Store does not apply its initial
    /// logs within the specified time.
    /// </summary>
    public class OpenTimeoutException : Exception
    {
        public OpenTimeoutException() : base("timeout waiting for initial logs application") { }
    }
    /// <summary>
    /// ErrInvalidBackupFormat is returned when the requested backup format
    /// is not valid.
    /// </summary>
    public class InvalidBackupFormatException : Exception
    {
        public InvalidBackupFormatException() : base("invalid backup format") { }
    }
    /// <summary>
    /// ErrTransactionActive is returned when an operation is blocked by an
    /// active transaction.
    /// </summary>
    public class TransactionActiveException : Exception
    {
        public TransactionActiveException() : base("transaction in progress") { }
    }
    /// <summary>
    /// ErrDefaultConnection is returned when an attempt is made to delete the
    /// default connection.
    /// </summary>
    public class DefaultConnectionException : Exception
    {
        public DefaultConnectionException() : base("cannot delete default connection") { }
    }
    /// <summary>
    /// ErrConnectionDoesNotExist is returned when an operation is attempted
    /// on a non-existent connection. This can happen if the connection
    /// was previously open but is now closed.
    /// </summary>
    public class ConnectionDoesNotExistException : Exception
    {
        public ConnectionDoesNotExistException() : base("connection does not exist") { }
    }
    /// <summary>
    /// ErrStoreInvalidState is returned when a Store is in an invalid
    /// state for the requested operation.
    /// </summary>
    public class StoreInvalidStateException : Exception
    {
        public StoreInvalidStateException() : base("store not in valid state") { }
    }
}