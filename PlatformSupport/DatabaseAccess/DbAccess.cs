
namespace PlatformSupport.DatabaseAccess
{
    internal abstract class DbAccess : IDbAccess
    {
        public abstract int FractionalSecondsSupported { get; }

        protected readonly string ConnectionString;

        protected DbAccess(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public abstract ITransaction BeginTransaction();

        public abstract ITransaction BeginTransaction(SupportedIsolation isolationLevel);

        public abstract ICommand CreateCommand();

        public abstract RecordIdentifier FetchId();

        public abstract RecordIdentifier FetchFirstIdOfReservedSet(int setCount);

        public void Dispose()
        {
            Dispose(true);   
        }

        protected abstract void Dispose(bool disposing);
    }
}
