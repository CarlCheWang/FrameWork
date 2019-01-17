using System.Data.Common;

namespace PlatformSupport.DatabaseAccess
{
    internal abstract class Transaction : ITransaction
    {
        protected readonly UsageEnforcer Enforcer;
        protected DbTransaction DbTransaction;

        internal Transaction(UsageEnforcer enforcer, DbTransaction transaction)
        {
            Enforcer = enforcer;
            DbTransaction = transaction;
        }

        public abstract void Commit();

        public void Rollback()
        {
            Enforcer.VerifyCanEndTransaction(this);  
            DbTransaction.Rollback();
            Enforcer.EndTransaction(this);
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected void Dispose(bool disposing)
        {
            using (DbTransaction)
            {
            }
            DbTransaction = null;

            Enforcer.VerifyTransactionIsDisposable(this);
            Enforcer.DisposeTransaction(this);
        }

    }
}
