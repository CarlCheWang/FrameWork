using System.Data.Common;

namespace PlatformSupport.DatabaseAccess
{
    internal sealed class MySqlTransactionWrapper : Transaction
    {
        private readonly MySqlDbAccess _mysqlDbAccess;
        private readonly SupportedIsolation _isolationLevel;

        internal MySqlTransactionWrapper(UsageEnforcer enforcer, MySqlDbAccess dbAccess, DbTransaction transaction)
            : base(enforcer, transaction)
        {
            _mysqlDbAccess = dbAccess;
            _isolationLevel = (SupportedIsolation)transaction.IsolationLevel;
        }

        public override void Commit()
        {
            Enforcer.VerifyCanEndTransaction(this);  
            DbTransaction.Commit();
            Enforcer.EndTransaction(this);
            _mysqlDbAccess.ResetToDefaultIsolationLevel(_isolationLevel);
        }

    }
}
