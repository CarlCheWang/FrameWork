using System;

namespace PlatformSupport.DatabaseAccess
{
    public interface IDbAccess : IDisposable
    {
        /// <summary>
        /// The maximum digits of fractional seconds supported by database's date time data types
        /// </summary>
        int FractionalSecondsSupported { get; }

        /// <summary>
        /// Begin an explicit local transaction with default isolation level. All existing (not disposed) commands on the connection will execute in this transaction.
        /// </summary>
        ITransaction BeginTransaction();

        /// <summary>
        /// Begin an explicit local transaction. All existing (not disposed) commands on the connection will execute in this transaction.
        /// </summary>
        /// <param name="isolationLevel">transaction isolation level</param>
        ITransaction BeginTransaction(SupportedIsolation isolationLevel);

        ICommand CreateCommand();   

        RecordIdentifier FetchId();

        RecordIdentifier FetchFirstIdOfReservedSet(int setCount);
    }
}
