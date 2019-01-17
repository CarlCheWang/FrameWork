using System;
using System.Data;
using System.Data.Common;

namespace PlatformSupport.DatabaseAccess
{
    internal sealed class MySqlDbAccess : DbAccess
    {
        private DbConnection _connection;
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

        public MySqlDbAccess(DbProviderFactory providerFactory, string connectionString)
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
                if (_connection.ServerVersion != null)
                {
                    // only mysql 5.6.4 or later supports frac sec
                    string[] pieces = _connection.ServerVersion.Split('.');
                    if (Convert.ToInt16(pieces[0]) > 5) return 6;
                    if (Convert.ToInt16(pieces[0]) == 5 && Convert.ToInt16(pieces[1]) > 6) return 6;
                    if (Convert.ToInt16(pieces[0]) == 5 && Convert.ToInt16(pieces[1]) == 6 && Convert.ToInt16(pieces[2]) >= 4) return 6;
                    return 0;
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
            var transaction = new MySqlTransactionWrapper(_enforcer, this, dbTransaction);
            _enforcer.CreatedTransaction(transaction);
            return transaction;      
        }

        public override ICommand CreateCommand()
        {
            _enforcer.VerifyValidDbAccess();
            var dbCommand = _connection.CreateCommand();
            var command = new MySqlCommandWrapper(_enforcer, dbCommand);
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
