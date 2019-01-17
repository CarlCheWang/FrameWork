using NUnit.Framework;

namespace PlatformSupport.DatabaseAccessComponentTests
{
    [TestFixture]
    public class FactoryTest : TestBase
    {
        [TestCaseSource("ConnectionData")]
        public void CreatesDbAccess(string providerName, string connectionString)
        {
            using (var db = Factory.CreateDbAccess(providerName, connectionString))
            {
                Assert.IsNotNull(db);
            }
        }
    }
}