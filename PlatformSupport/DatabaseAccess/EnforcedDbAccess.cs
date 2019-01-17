using System;

namespace PlatformSupport.DatabaseAccess
{
    internal class EnforcedDbAccess
    {
        internal IDbAccess DbAccess { get; set; }

        internal Exception InnerException { get; set; }

        internal EnforcedDbAccess(IDbAccess dbAccess)
        {
            DbAccess = dbAccess;
        }
    }
}
