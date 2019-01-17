using System;

namespace PlatformSupport.DatabaseAccess
{
    public interface ITransaction : IDisposable
    {
        void Commit();

        void Rollback();
    }
}
