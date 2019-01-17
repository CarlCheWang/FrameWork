using System;
using System.Data;
using System.Data.Common;

namespace PlatformSupport.DatabaseAccess
{
    internal sealed class SqlServerDbAccess : DbAccess
    {
        private DbConnection _connection;
        private DbTransaction _currentTransaction;
        private readonly UsageEnforcer _enforcer;

        // reset isolation level to default when ending an explicit transaction
        // otherwise isolation level gets used by next implicit transaction, and is leaked across connections in a pool
        internal void ResetToDefaultIsolationLevel(SupportedIsolation currentLevel)
        {
            if (currentLevel != SupportedIsolation.Default)
            {
                using (var trans = BeginTransaction(SupportedIsolation.Default))
                {
                    trans.Commit();
                }
            }
        }   

        public SqlServerDbAccess(DbProviderFactory providerFactory, string connectionString)
            : base(connectionString)
        {
            _enforcer = new UsageEnforcer(this);
            _connection = providerFactory.CreateConnection();
            if (_connection != null)
            {
                _connection.ConnectionString = ConnectionString;
                _connection.Open();
            }
        }

        public override int FractionalSecondsSupported
        {
            get
            {
                // only sql server 2008 (10.x.x) or later supports frac sec
                if (_connection.ServerVersion != null)
                {
                    string[] pieces = _connection.ServerVersion.Split('.');
                    return Convert.ToInt16(pieces[0]) > 9 ? 7 : 0;
                }
                return 0;
            }
        }

        public override ITransaction BeginTransaction()
        {
            return BeginTransaction(SupportedIsolation.Default);
        }

        public override ITransaction BeginTransaction(SupportedIsolation isolationLevel)
        {
            _enforcer.VerifyCanBeginTransaction();
            var dbTransaction = _connection.BeginTransaction((IsolationLevel)isolationLevel);
            _currentTransaction = dbTransaction;
            var transaction = new SqlServerTransactionWrapper(_enforcer, this, dbTransaction);
            _enforcer.CreatedTransaction(transaction);
            return transaction;
        }

        public override ICommand CreateCommand()
        {
            _enforcer.VerifyValidDbAccess();
            var dbCommand = _connection.CreateCommand();
            dbCommand.Transaction = _currentTransaction;
            var command = new SqlServerCommandWrapper(_enforcer, dbCommand);
            _enforcer.CreatedCommand(command);
            return command;
        }

        public override RecordIdentifier FetchId()
        {
            throw new NotImplementedException();
        }

        public override RecordIdentifier FetchFirstIdOfReservedSet(int setCount)
        {
            throw new NotImplementedException();
        }

        protected override void Dispose(bool disposing)
        {
            using (_connection) { }
            _connection = null;
            _currentTransaction = null;

            _enforcer.VerifyDbAccessIsDisposable();
            _enforcer.DisposeDbAccess();
        }
    }
}
