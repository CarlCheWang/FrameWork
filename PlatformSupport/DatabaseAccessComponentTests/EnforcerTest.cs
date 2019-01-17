using NUnit.Framework;
using PlatformSupport.DatabaseAccess;

namespace PlatformSupport.DatabaseAccessComponentTests
{
    [TestFixture]
    public class EnforcerTest : TestBase
    {
        [TestCaseSource("ConnectionData")]
        public void EnforcerAllowsDbAccessDisposal(string providerName, string connectionString)
        {
            WithADbAccessRun(providerName, connectionString,
                             db => { });

            WithADbAccessRun(providerName, connectionString,
                             db =>
                                 {
                                     var cmd = db.CreateCommand();
                                     cmd.Dispose();
                                 });

            WithADbAccessRun(providerName, connectionString,
                             db =>
                                 {
                                     using (var trans = db.BeginTransaction())
                                     {
                                         trans.Rollback();
                                     }

                                     using (db.CreateCommand())
                                     {
                                     }

                                     var trans2 = db.BeginTransaction();
                                     trans2.Rollback();
                                     trans2.Dispose();

                                     var cmd = db.CreateCommand();
                                     cmd.Dispose();
                                 });
        }

        [TestCaseSource("ConnectionData")]
        public void EnforcerAllowsTransactionDisposal(string providerName, string connectionString)
        {
            WithADbAccessRun(providerName, connectionString,
                             db =>
                                 {
                                     using (var trans = db.BeginTransaction(SupportedIsolation.Default))
                                     {
                                         trans.Rollback();
                                     }

                                     var trans1 = db.BeginTransaction(SupportedIsolation.Serializable);
                                     trans1.Rollback();
                                     trans1.Dispose();
                                 });

            WithADbAccessRun(providerName, connectionString,
                             db =>
                                 {
                                     using (db.CreateCommand())
                                     {
                                     }

                                     var cmd = db.CreateCommand();
                                     cmd.Dispose();

                                     using (var trans = db.BeginTransaction(SupportedIsolation.Default))
                                     {
                                         trans.Rollback();
                                     }
                                 });
        }

        [TestCaseSource("ConnectionData")]
        public void EnforcerAllowsCommandDisposal(string providerName, string connectionString)
        {
            WithACommandRun(providerName, connectionString,
                            cmd => { });

            WithACommandRun(providerName, connectionString,
                            cmd =>
                                {
                                    cmd.CommandText = @"select val from scalar_test where val is null";
                                    cmd.ExecuteScalar();
                                });

            WithACommandRun(providerName, connectionString,
                            cmd =>
                                {
                                    cmd.CommandText = @"update ten_row_tab set value = value + 1";
                                    cmd.ExecuteNonQuery();
                                });

            WithACommandRun(providerName, connectionString,
                            cmd =>
                                {
                                    cmd.CommandText = @"select val from scalar_test";
                                    using (cmd.ExecuteReader())
                                    {
                                    }

                                    var reader = cmd.ExecuteReader();
                                    reader.Dispose();
                                });
        }
    }
}