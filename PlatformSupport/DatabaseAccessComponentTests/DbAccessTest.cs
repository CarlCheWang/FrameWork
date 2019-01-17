using System;
using NUnit.Framework;
using PlatformSupport.DatabaseAccess;

namespace PlatformSupport.DatabaseAccessComponentTests
{
    [TestFixture]
    public class DbAccessTest : TestBase
    {
        [TestCaseSource(nameof(ConnectionNameAndData))]
        public void GetsCorrectFractionalSecondsSupportedByDb(string configName, string providerName,
                                                              string connectionString)
        {
            WithADbAccessRun(providerName, connectionString,
                             db =>
                                 {
                                     int expected = 0;
                                     switch (configName)
                                     {
                                         case "Oracle":
                                             expected = 9;
                                             break;
                                         case "SqlServ2008":
                                             expected = 7;
                                             break;
                                         case "MySql5.5.19":
                                             expected = 0;
                                             break;
                                         case "MySql5.6.11":
                                             expected = 6;
                                             break;
                                     }

                                     Assert.AreEqual(expected, db.FractionalSecondsSupported);
                                 });
        }

        [TestCaseSource("ConnectionData")]
        public void CannotBeginTransactionWithUndisposedCommand(string providerName, string connectionString)
        {
            Assert.Throws(
                Is.TypeOf<InvalidOperationException>()
                  .And.Message.EqualTo("Cannot begin transaction when there are undisposed commands."),
                () =>
                WithADbAccessRun(providerName, connectionString,
                                 db =>
                                     {
                                         using (db.CreateCommand())
                                         {
                                             using (db.BeginTransaction(SupportedIsolation.Default))
                                             {
                                             }
                                         }
                                     }));

            // test when inner exception
            Assert.Throws(
                Is.TypeOf<InvalidOperationException>()
                  .And.InnerException.Message.EqualTo("Cannot begin transaction when there are undisposed commands."),
                () =>
                WithADbAccessRun(providerName, connectionString,
                                 db =>
                                     {
                                         db.CreateCommand();
                                         db.BeginTransaction(SupportedIsolation.Default);
                                     }
                    ));
        }

        [TestCaseSource("ConnectionData")]
        public void CannotBeginTransWhenConnectionAlreadyHasPendingTrans(string providerName, string connectionString)
        {
            Assert.Throws(
                Is.TypeOf<InvalidOperationException>()
                  .And.InnerException.Message.EqualTo("Cannot begin transaction when there is pending transaction."),
                () =>
                WithADbAccessRun(providerName, connectionString,
                                 db =>
                                     {
                                         db.BeginTransaction();

                                         // before trans has been ended (commit/rollback), try to begin another transaction
                                         using (db.BeginTransaction())
                                         {
                                         }
                                     }));
        }

        // todo: cannot be tested since sqlCe offers no way to verify isolation level using database level views; how to deal with this?
        [TestCaseSource("ConnectionAndIsolationData")]
        public void BeginsTransactionDefaultIsolationLevel(string providerName, string connectionString,
                                                           string readCommittedLiteral, string serializableLiteral)
        {
            var id = Guid.NewGuid();
            WithADbAccessRun(providerName, connectionString,
                             db =>
                                 {
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
            Assert.AreEqual(readCommittedLiteral, GetRecordedIsolationLevel(providerName, connectionString, id));
            DeleteTransIsolationTestData(providerName, connectionString, id);
        }

        // todo: cannot be tested since sqlCe offers no way to verify isolation level using database level views; how to deal with this?
        [TestCaseSource("ConnectionAndIsolationData")]
        public void BeginsTransactionSpecifiedIsolationLevel(string providerName, string connectionString,
                                                             string readCommittedLiteral, string serializableLiteral)
        {
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();

            WithADbAccessRun(providerName, connectionString,
                             db =>
                                 {
                                     using (var trans = db.BeginTransaction(SupportedIsolation.Serializable))
                                     {
                                         using (var cmd = db.CreateCommand())
                                         {
                                             cmd.CommandText = @"insert into trans_tab (id, value) values ('" + id1 +
                                                               @"','one')";
                                             cmd.ExecuteNonQuery();
                                         }
                                         trans.Commit();
                                     }

                                     using (var trans = db.BeginTransaction(SupportedIsolation.Default))
                                     {
                                         using (var cmd = db.CreateCommand())
                                         {
                                             cmd.CommandText = @"insert into trans_tab (id, value) values ('" + id2 +
                                                               @"','one')";
                                             cmd.ExecuteNonQuery();
                                         }
                                         trans.Commit();
                                     }
                                 });

            Assert.AreEqual(serializableLiteral, GetRecordedIsolationLevel(providerName, connectionString, id1));
            Assert.AreEqual(readCommittedLiteral, GetRecordedIsolationLevel(providerName, connectionString, id2));
            DeleteTransIsolationTestData(providerName, connectionString, id1);
            DeleteTransIsolationTestData(providerName, connectionString, id2);
        }

        // todo: for sqlCe the test can only be a check of transaction object isolation level property, since sqlCe offers no way to verify isolation level using database level views; how to deal with this?
        [TestCaseSource("ConnectionAndIsolationData")]
        public void CorrectIsolationLevelUsedForExplicitTransactionsInSameConnection(string providerName,
                                                                                     string connectionString,
                                                                                     string readCommittedLiteral,
                                                                                     string serializableLiteral)
        {
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();

            WithADbAccessRun(providerName, connectionString,
                             db =>
                                 {
                                     // serializable isolation level
                                     using (var trans = db.BeginTransaction(SupportedIsolation.Serializable))
                                     {
                                         using (var cmd = db.CreateCommand())
                                         {
                                             cmd.CommandText = @"insert into trans_tab (id, value) values ('" + id1 +
                                                               @"','one')";
                                             cmd.ExecuteNonQuery();
                                         }
                                         trans.Commit();
                                     }

                                     // default isolation level
                                     using (var trans = db.BeginTransaction())
                                     {
                                         using (var cmd = db.CreateCommand())
                                         {
                                             cmd.CommandText = @"insert into trans_tab (id, value) values ('" + id2 +
                                                               @"','one')";
                                             cmd.ExecuteNonQuery();
                                         }
                                         trans.Commit();
                                     }
                                 });

            Assert.AreEqual(serializableLiteral, GetRecordedIsolationLevel(providerName, connectionString, id1));
            Assert.AreEqual(readCommittedLiteral, GetRecordedIsolationLevel(providerName, connectionString, id2));
            DeleteTransIsolationTestData(providerName, connectionString, id1);
            DeleteTransIsolationTestData(providerName, connectionString, id2);
        }

        // todo: cannot be tested since sqlCe offers no way to verify isolation level using database level views; how to deal with this?
        [TestCaseSource("ConnectionAndIsolationData")]
        public void CorrectIsolationLevelUsedForImplicitTransactionsInSameConnection(string providerName,
                                                                                     string connectionString,
                                                                                     string readCommittedLiteral,
                                                                                     string serializableLiteral)
        {
            if (providerName == "System.Data.SqlServerCe.3.5") Assert.Ignore();

            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();

            WithADbAccessRun(providerName, connectionString,
                             db =>
                                 {
                                     // explicit transactions with serializable isolation level                  
                                     using (var trans = db.BeginTransaction(SupportedIsolation.Serializable))
                                     {
                                         using (var cmd = db.CreateCommand())
                                         {
                                             cmd.CommandText = @"insert into trans_tab (id, value) values ('" + id1 +
                                                               @"','one')";
                                             cmd.ExecuteNonQuery();
                                         }
                                         trans.Commit();
                                     }

                                     // implicit transaction, should use default isolation level
                                     using (var cmd = db.CreateCommand())
                                     {
                                         cmd.CommandText = @"insert into trans_tab (id, value) values ('" + id2 +
                                                           @"','one')";
                                         cmd.ExecuteNonQuery();
                                     }
                                 });

            Assert.AreEqual(serializableLiteral, GetRecordedIsolationLevel(providerName, connectionString, id1));
            Assert.AreEqual(readCommittedLiteral, GetRecordedIsolationLevel(providerName, connectionString, id2));
            DeleteTransIsolationTestData(providerName, connectionString, id1);
            DeleteTransIsolationTestData(providerName, connectionString, id2);
        }

        [TestCaseSource("ConnectionData")]
        public void CreatesCommand(string providerName, string connectionString)
        {
            WithADbAccessRun(providerName, connectionString,
                             db =>
                                 {
                                     using (var cmd = db.CreateCommand())
                                     {
                                         Assert.That(cmd, Is.Not.Null);
                                     }
                                 });
        }

        [TestCaseSource("ConnectionData")]
        public void GetsNextIdentifier(string providerName, string connectionString)
        {
            WithADbAccessRun(providerName, connectionString,
                             db =>
                                 {
                                     var id = db.FetchId();
                                     Assert.AreNotEqual(0, id);
                                     var nextId = db.FetchId();
                                     Assert.IsTrue(nextId > id);
                                 });
        }

        [TestCaseSource("ConnectionData")]
        public void GetsNextSetsOfIdentifiersInCorrectIncrements(string providerName, string connectionString)
        {
            const int setCount = 10;

            WithADbAccessRun(providerName, connectionString,
                             db =>
                                 {
                                     var setFirstId = db.FetchFirstIdOfReservedSet(setCount);
                                     Assert.AreNotEqual(0, setFirstId);
                                     var nextSetFirstId = db.FetchFirstIdOfReservedSet(setCount);
                                     Assert.AreNotEqual(0, nextSetFirstId);
                                     Assert.AreEqual(setFirstId + setCount, nextSetFirstId);
                                 });
        }


        // todo: cannot test for sqlCe; does not support connection pooling.  how to do this?
        [TestCaseSource("ConnectionAndIsolationData")]
        public void CorrectTransactionIsolationLevelUsedForImplicitTransactionsAcrossConnectionsInPool(
            string providerName, string connectionString, string readCommittedLiteral, string serializableLiteral)
        {
            // a connection that uses serializable transaction
            var id1 = Guid.NewGuid();
            WithADbAccessRun(providerName, connectionString,
                             db =>
                                 {
                                     using (var trans = db.BeginTransaction(SupportedIsolation.Serializable))
                                     {
                                         using (var cmd = db.CreateCommand())
                                         {
                                             cmd.CommandText = @"insert into trans_tab (id, value) values ('" + id1 +
                                                               @"','one')";
                                             cmd.ExecuteNonQuery();
                                         }
                                         trans.Commit();
                                     }
                                 });

            // make sure serializable was used in last dml
            Assert.AreEqual(serializableLiteral, GetRecordedIsolationLevel(providerName, connectionString, id1));
            DeleteTransIsolationTestData(providerName, connectionString, id1);

            // open a specified number of connections to execute dml with implicit transactions
            const int connectionsToTry = 10;
            var cnt = 0;
            while (cnt < connectionsToTry)
            {
                var id2 = Guid.NewGuid();
                WithACommandRun(providerName, connectionString,
                                cmd =>
                                    {
                                        cmd.CommandText =
                                            String.Format("insert into trans_tab (id, value) values ('{0}','foo')", id2);
                                        cmd.ExecuteNonQuery();
                                    });

                // make sure read committed was used by dml
                Assert.AreEqual(readCommittedLiteral, GetRecordedIsolationLevel(providerName, connectionString, id2));
                DeleteTransIsolationTestData(providerName, connectionString, id2);

                cnt++;
            }
        }

        [TestCaseSource("ConnectionData")]
        public void SeparateConnectionsTransactionControlDoNotAffectEachOther(string providerName,
                                                                              string connectionString)
        {
            var id1 = Guid.NewGuid();
            var id2 = Guid.NewGuid();
            var id3 = Guid.NewGuid();
            // conn1, explicit transaction rolled back
            WithADbAccessRun(providerName, connectionString,
                             db1 =>
                                 {
                                     using (var trans1 = db1.BeginTransaction())
                                     {
                                         // conn2, explicit trans rolled back
                                         WithADbAccessRun(providerName, connectionString,
                                                          db2 =>
                                                              {
                                                                  using (
                                                                      var trans2 =
                                                                          db2.BeginTransaction(
                                                                              SupportedIsolation.Serializable))
                                                                  {
                                                                      using (var cmd2 = db2.CreateCommand())
                                                                      {
                                                                          cmd2.CommandText =
                                                                              @"insert into trans_tab (id, value) values ('" +
                                                                              id2 + @"','one')";
                                                                          cmd2.ExecuteNonQuery();
                                                                      }
                                                                      trans2.Rollback();
                                                                  }
                                                              });

                                         // conn3, implicit trans will commit
                                         WithADbAccessRun(providerName, connectionString,
                                                          db3 =>
                                                              {
                                                                  using (var cmd3 = db3.CreateCommand())
                                                                  {
                                                                      cmd3.CommandText =
                                                                          @"insert into trans_tab (id, value) values ('" +
                                                                          id3 + @"','one')";
                                                                      cmd3.ExecuteNonQuery();
                                                                  }
                                                              });

                                         using (var cmd1 = db1.CreateCommand())
                                         {
                                             cmd1.CommandText = @"insert into trans_tab (id, value) values ('" + id1 +
                                                                @"','one')";
                                             cmd1.ExecuteNonQuery();
                                         }
                                         trans1.Rollback();
                                     }
                                 });

            // do check using separate connection
            Assert.IsTrue(0 == GetTransTableRowCount(providerName, connectionString, id1));
            Assert.IsTrue(0 == GetTransTableRowCount(providerName, connectionString, id2));
            Assert.IsTrue(1 == GetTransTableRowCount(providerName, connectionString, id3));
            DeleteTransIsolationTestData(providerName, connectionString, id1);
            DeleteTransIsolationTestData(providerName, connectionString, id2);
            DeleteTransIsolationTestData(providerName, connectionString, id3);
        }

        [TestCaseSource("ConnectionData")]
        public void CannotDisposeWhenUndisposedTransOrCommand(string providerName, string connectionString)
        {
            Assert.Throws(Is.TypeOf<InvalidOperationException>(), () =>
                                                                  WithADbAccessRun(providerName, connectionString,
                                                                                   db => db.BeginTransaction()));
            Assert.Throws(Is.TypeOf<InvalidOperationException>(), () =>
                                                                  WithADbAccessRun(providerName, connectionString,
                                                                                   db => db.CreateCommand()));
        }

        [TestCaseSource("ConnectionData")]
        public void IsDisposed(string providerName, string connectionString)
        {
            var db = Factory.CreateDbAccess(providerName, connectionString);
            Assert.IsNotNull(db);
            db.Dispose();
            Assert.Throws(Is.TypeOf<ObjectDisposedException>(), () => db.BeginTransaction());
        }
    }
}