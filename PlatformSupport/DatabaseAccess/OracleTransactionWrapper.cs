using System.Data.Common;

namespace PlatformSupport.DatabaseAccess
{
    internal sealed class OracleTransactionWrapper : Transaction
    {
        internal OracleTransactionWrapper(UsageEnforcer enforcer, DbTransaction transaction) : base(enforcer, transaction)
        {
        }

        public override void Commit()
        {
            Enforcer.VerifyCanEndTransaction(this);  
            DbTransaction.Commit();
            Enforcer.EndTransaction(this);
        }
    }
}
