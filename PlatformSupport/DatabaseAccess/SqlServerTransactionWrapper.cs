using System.Data.Common;

namespace PlatformSupport.DatabaseAccess
{
    internal sealed class SqlServerTransactionWrapper : Transaction
    {
        private readonly SqlServerDbAccess _sqlDbAccess;
        private readonly SupportedIsolation _isolationLevel;

        internal SqlServerTransactionWrapper(UsageEnforcer enforcer, SqlServerDbAccess dbAccess, DbTransaction transaction)
            : base(enforcer, transaction)
        {
            _sqlDbAccess = dbAccess;
            _isolationLevel = (SupportedIsolation)transaction.IsolationLevel;
        }

        public override void Commit()
        {
            Enforcer.VerifyCanEndTransaction(this);  
            DbTransaction.Commit();
            Enforcer.EndTransaction(this);
            _sqlDbAccess.ResetToDefaultIsolationLevel(_isolationLevel);
        }

    }
}
