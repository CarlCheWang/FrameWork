using System;
using System.Data;
using NUnit.Framework;
using PlatformSupport.DatabaseAccess;

namespace PlatformSupport.DatabaseAccessComponentTests
{
    [TestFixture]
    public class CommandTest : TestBase
    {
        [TestCaseSource("ConnectionData")]
        public void SetsCommandType(string providerName, string connectionString)
        {
            WithACommandRun(providerName, connectionString,
                            cmd =>
                                {
                                    cmd.CommandType = SupportedCommandType.Text;
                                    Assert.AreEqual(SupportedCommandType.Text, cmd.CommandType);
                                });

            WithACommandRun(providerName, connectionString,
                            cmd =>
                                {
                                    cmd.CommandType = SupportedCommandType.StoredProcedure;
                                    Assert.AreEqual(SupportedCommandType.StoredProcedure, cmd.CommandType);
                                });
        }

        [TestCaseSource("ConnectionData")]
        public void SetsCommandText(string providerName, string connectionString)
        {
            const string text = @"not a real sql statement";

            WithACommandRun(providerName, connectionString,
                            cmd =>
                                {
                                    cmd.CommandText = text;
                                    Assert.AreEqual(text, cmd.CommandText);
                                });
        }

        [TestCaseSource("ConnectionData")]
        public void GetsAndSetsCommandTimeout(string providerName, string connectionString)
        {
            WithACommandRun(providerName, connectionString,
                            cmd =>
                                {
                                    cmd.CommandTimeout = 99;
                                    Assert.AreEqual(99, cmd.CommandTimeout);
                                });
        }

        [TestCaseSource("ConnectionData")]
        public void ExecutesDmlReturningRowsAffected(string providerName, string connectionString)
        {
            const int expected = 10;
            const string text = @"update ten_row_tab set value = value + 1";

            WithACommandRun(providerName, connectionString,
                            cmd =>
                                {
                                    cmd.CommandText = text;
                                    var result = cmd.ExecuteNonQuery();
                                    Assert.AreEqual(expected, result);
                                });
        }

        [TestCaseSource("ConnectionData")]
        public void CreatesReader(string providerName, string connectionString)
        {
            const string text = @"select id, value from ten_row_tab";

            WithACommandRun(providerName, connectionString,
                            cmd =>
                                {
                                    cmd.CommandText = text;
                                    using (var reader = cmd.ExecuteReader())
                                    {
                                        Assert.IsNotNull(reader);
                                    }
                                });
        }

        [TestCaseSource("ConnectionData")]
        public void CreatesReaderWithCommandBehaviour(string providerName, string connectionString)
        {
            const string text = @"select id, value from ten_row_tab";

            WithACommandRun(providerName, connectionString,
                            cmd =>
                            {
                                cmd.CommandText = text;
                                using (var reader = cmd.ExecuteReader(CommandBehavior.SchemaOnly))
                                {
                                    var schemaTable = reader.GetSchemaTable();

                                    var firstColumnName = schemaTable.Rows[0]["ColumnName"];
                                    Assert.That(firstColumnName, Is.EqualTo("id").IgnoreCase);

                                    var firstColumnType = schemaTable.Rows[0]["DataType"];
                                    Assert.That(new []{firstColumnType}, Is.SubsetOf(new []{typeof(int), typeof(decimal)}));

                                    var secondColumnName = schemaTable.Rows[1]["ColumnName"];
                                    Assert.That(secondColumnName, Is.EqualTo("value").IgnoreCase);

                                    var secondColumnType = schemaTable.Rows[1]["DataType"];
                                    Assert.That(new[] { secondColumnType }, Is.SubsetOf(new[] { typeof(Int64), typeof(decimal)}));
                                }
                            });
        }

        [TestCaseSource("ConnectionData")]
        public void ReturnsCorrectScalarResult(string providerName, string connectionString)
        {
            const string noResult = @"select val from scalar_test where val is null";
            const string nullResult = @"select null from scalar_test";
            const string valZero = @"select val from scalar_test";

            WithACommandRun(providerName, connectionString,
                            cmd =>
                                {
                                    cmd.CommandText = nullResult;
                                    Assert.AreEqual(DBNull.Value, cmd.ExecuteScalar());

                                    cmd.CommandText = valZero;
                                    Assert.IsTrue(0 == Convert.ToInt16(cmd.ExecuteScalar()));

                                    cmd.CommandText = noResult;
                                    Assert.AreEqual(DBNull.Value, cmd.ExecuteScalar());
                                });
        }

        [TestCaseSource("ConnectionData")]
        public void CannotDisposeWhenUndisposedReader(string providerName, string connectionString)
        {
            Assert.Throws(
                Is.TypeOf<InvalidOperationException>()
                  .And.InnerException.Message.EqualTo("Cannot dispose command when there are undisposed readers."),
                () =>
                WithACommandRun(providerName, connectionString,
                                cmd =>
                                    {
                                        cmd.CommandText = @"select * from ten_row_tab";
                                        cmd.ExecuteReader();
                                        // reader is not disposed
                                    }));
        }

        [TestCaseSource("ConnectionData")]
        public void IsDisposed(string providerName, string connectionString)
        {
            Assert.Throws(Is.TypeOf<ObjectDisposedException>(), () =>
                                                                WithADbAccessRun(providerName, connectionString,
                                                                                 db =>
                                                                                     {
                                                                                         var cmd = db.CreateCommand();
                                                                                         cmd.CommandText =
                                                                                             @"not a real sql statement";
                                                                                         cmd.Dispose();
                                                                                         cmd.ExecuteNonQuery();
                                                                                     }));
        }
    }
}