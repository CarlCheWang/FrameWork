using System.Data.Common;

namespace PlatformSupport.DatabaseAccess
{
    /// <summary>
    /// Adapts static DbProviderFactories class so that it can be mocked for testing.
    /// </summary>
    public class ProviderFactoriesAdapter
    {
        public virtual DbProviderFactory GetFactory(string providerName)
        {
            return DbProviderFactories.GetFactory(providerName);
        }
    }
}