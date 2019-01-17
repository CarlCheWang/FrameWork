using System;

namespace PlatformSupport.DatabaseAccess
{
    internal class EnforcedTransaction
    {
        internal ITransaction Transaction { get; set; }

        internal bool Ended { get; set; }

        internal Exception InnerException { get; set; }

        internal EnforcedTransaction(ITransaction transaction, bool ended)
        {
            Transaction = transaction;
            Ended = ended;
        }        
    }
}
