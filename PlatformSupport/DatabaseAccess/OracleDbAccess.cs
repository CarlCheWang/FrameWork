using System;
using System.Data;
using System.Data.Common;

namespace PlatformSupport.DatabaseAccess
{
    internal sealed class OracleDbAccess : DbAccess
    {
        private DbConnection _connection;
        private readonly UsageEnforcer _enforcer;

        public OracleDbAccess(DbProviderFactory providerFactory, string connectionString) : base(connectionString)
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
            // all versions of oracle from 9i onward support frac sec up to 9 decimal places
            get { return 9; }
        }

        public override ITransaction BeginTransaction()
        {
            return BeginTransaction(SupportedIsolation.Default);
        }

        public override ITransaction BeginTransaction(SupportedIsolation isolationLevel)
        {
            _enforcer.VerifyCanBeginTransaction();
            var dbTransaction = _connection.BeginTransaction((IsolationLevel)isolationLevel);
            var transaction = new OracleTransactionWrapper(_enforcer, dbTransaction);
            _enforcer.CreatedTransaction(transaction);
            return transaction;      
        }

        public override ICommand CreateCommand()
        {
            _enforcer.VerifyValidDbAccess();
            var dbCommand = _connection.CreateCommand();
            var command = new OracleCommandWrapper(_enforcer, dbCommand);
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
            using(_connection){}
            _connection = null;

            _enforcer.VerifyDbAccessIsDisposable();
            _enforcer.DisposeDbAccess();
        }
    }
}
