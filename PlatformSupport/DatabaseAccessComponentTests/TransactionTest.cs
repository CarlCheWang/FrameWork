using System;
using NUnit.Framework;
using PlatformSupport.DatabaseAccess;

namespace PlatformSupport.DatabaseAccessComponentTests
{
    [TestFixture]
    public class TransactionTest : TestBase
    {
        [TestCaseSource("ConnectionData")]
        public void Commits(string providerName, string connectionString)
        {
            var id = Guid.NewGuid();
            WithADbAccessRun(providerName, connectionString,
                             db =>
                                 {
                                     // insert and commit
                                     using (var trans = db.BeginTransaction())
                                     {
                                         using (var cmd = db.CreateCommand())
                                         {
                                             cmd.CommandText = @"insert into trans_tab (id, value) values ('" + id +
                                                               @"','one')";
                                             cmd.ExecuteNonQuery();
                                         }
                                         trans.Commit();
                                     }
                                 });

            // do check using separate connection
            Assert.IsTrue(1 == GetTransTableRowCount(providerName, connectionString, id));
            DeleteTransIsolationTestData(providerName, connectionString, id);
        }

        [TestCaseSource("ConnectionData")]
        public void RollsBack(string providerName, string connectionString)
        {
            var id = Guid.NewGuid();
            WithADbAccessRun(providerName, connectionString,
                             db =>
                                 {
                                     // insert and rollback
                                     using (var trans = db.BeginTransaction())
                                     {
                                         using (var cmd = db.CreateCommand())
                                         {
                                             cmd.CommandText = @"insert into trans_tab (id, value) values ('" + id +
                                                               @"','one')";
                                             cmd.ExecuteNonQuery();
                                         }
                                         trans.Rollback();
                                     }
                                 });

            // do check using separate connection
            Assert.IsTrue(0 == GetTransTableRowCount(providerName, connectionString, id));
            DeleteTransIsolationTestData(providerName, connectionString, id);
        }

        [TestCaseSource("ConnectionData")]
        public void CannotEndTransactionMoreThanOnce(string providerName, string connectionString)
        {
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();
            Assert.Throws(
                Is.TypeOf<InvalidOperationException>()
                  .And.Message.EqualTo(@"Transaction has already ended; it is no longer usable."),
                () => WithADbAccessRun(providerName, connectionString, db =>
                    {
                        using (var trans = db.BeginTransaction())
                        {
                            using (var cmd = db.CreateCommand())
                            {
                                cmd.CommandText =
                                    @"insert into trans_tab (id, value) values ('" + id1 + @"','one')";
                                cmd.ExecuteNonQuery();
                            }
                            trans.Rollback();
                            trans.Commit();
                        }
                    }));

            Assert.Throws(
                Is.TypeOf<InvalidOperationException>()
                  .And.Message.EqualTo(@"Transaction has already ended; it is no longer usable."),
                () => WithADbAccessRun(providerName, connectionString, db =>
                    {
                        using (var trans = db.BeginTransaction())
                        {
                            using (var cmd = db.CreateCommand())
                            {
                                cmd.CommandText =
                                    @"insert into trans_tab (id, value) values ('" + id2 + @"','one')";
                                cmd.ExecuteNonQuery();
                            }
                            trans.Commit();
                            trans.Rollback();
                        }
                    }));

            DeleteTransIsolationTestData(providerName, connectionString, id1);
            DeleteTransIsolationTestData(providerName, connectionString, id2);
        }

        [TestCaseSource("ConnectionData")]
        public void CannotDisposeUnendedTransaction(string providerName, string connectionString)
        {
            var id = Guid.NewGuid();
            Assert.Throws(
                Is.TypeOf<InvalidOperationException>()
                  .And.InnerException.Message.EqualTo(
                      "Cannot dispose transaction which has not been committed or rolled back."),
                () =>
                WithADbAccessRun(providerName, connectionString,
                                 db =>
                                     {
                                         using (db.BeginTransaction())
                                         {
                                             using (var cmd = db.CreateCommand())
                                             {
                                                 cmd.CommandText = @"insert into trans_tab (id, value) values ('" + id +
                                                                   @"','one')";
                                                 cmd.ExecuteNonQuery();
                                             }
                                             // no commit/rollback
                                         }
                                     }));

            // verify transaction rolled back using separate connection
            Assert.IsTrue(0 == GetTransTableRowCount(providerName, connectionString, id));
            DeleteTransIsolationTestData(providerName, connectionString, id);
        }

        // 2 of these tests, to make sure an error on disposing transaction actually does
        // rollback and dispose the underlying db transaction
        [TestCaseSource("ConnectionData")]
        public void CannotDisposeTransactionWithUndisposedCommand1(string providerName, string connectionString)
        {
            var id = Guid.NewGuid();
            Assert.Throws(
                Is.TypeOf<InvalidOperationException>()
                  .And.InnerException.Message.EqualTo("Cannot dispose transaction when there are undisposed commands."),
                () =>
                WithADbAccessRun(providerName, connectionString,
                                 db =>
                                     {
                                         using (db.BeginTransaction())
                                         {
                                             var cmd = db.CreateCommand();
                                             cmd.CommandText = @"insert into trans_tab (id, value) values ('" + id +
                                                               @"','one')";
                                             cmd.ExecuteNonQuery();
                                         }
                                     }));

            // do check using separate connection
            Assert.IsTrue(0 == GetTransTableRowCount(providerName, connectionString, id));
            DeleteTransIsolationTestData(providerName, connectionString, id);
        }

        [TestCaseSource("ConnectionData")]
        public void CannotDisposeTransactionWithUndisposedCommand2(string providerName, string connectionString)
        {
            CannotDisposeTransactionWithUndisposedCommand1(providerName, connectionString);
        }

        [TestCaseSource("ConnectionData")]
        public void IsDisposed(string providerName, string connectionString)
        {
            var id = Guid.NewGuid();
            Assert.Throws(Is.TypeOf<ObjectDisposedException>(), () =>
                                                                WithADbAccessRun(providerName, connectionString,
                                                                                 db =>
                                                                                     {
                                                                                         var trans =
                                                                                             db.BeginTransaction(
                                                                                                 SupportedIsolation
                                                                                                     .Serializable);

                                                                                         using (
                                                                                             var cmd =
                                                                                                 db.CreateCommand())
                                                                                         {
                                                                                             cmd.CommandText =
                                                                                                 @"insert into trans_tab (id, value) values ('" +
                                                                                                 id + @"','one')";
                                                                                             cmd.ExecuteNonQuery();
                                                                                         }

                                                                                         trans.Rollback();
                                                                                         trans.Dispose();

                                                                                         trans.Commit();
                                                                                     }));

            // do check using separate connection
            Assert.IsTrue(0 == GetTransTableRowCount(providerName, connectionString, id));
            DeleteTransIsolationTestData(providerName, connectionString, id);
        }
    }
}
