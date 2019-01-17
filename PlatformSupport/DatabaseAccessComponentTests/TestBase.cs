using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using NUnit.Framework;
using PlatformSupport.DatabaseAccess;

namespace PlatformSupport.DatabaseAccessComponentTests
{
    public class TestBase
    {
        protected const int MysqlUTinyIntMin = Byte.MinValue;
        protected const int MysqlUTinyIntMax = Byte.MaxValue;
        protected const int MysqlTinyIntMin = SByte.MinValue;
        protected const int MysqlTinyIntMax = SByte.MaxValue;
        protected const int MysqlUSmallIntMin = UInt16.MinValue;
        protected const int MysqlUSmallIntMax = UInt16.MaxValue;
        protected const int MysqlSmallIntMin = Int16.MinValue;
        protected const int MysqlSmallIntMax = Int16.MaxValue;
        protected const int MysqlUMediumIntMin = 0;
        protected const int MysqlUMediumIntMax = 16777215;
        protected const int MysqlMediumIntMin = -8388608;
        protected const int MysqlMediumIntMax = 8388607;
        protected const uint MysqlUIntMin = uint.MinValue;
        protected const uint MysqlUIntMax = uint.MaxValue;
        protected const int MysqlIntMin = int.MinValue;
        protected const int MysqlIntMax = int.MaxValue;
        protected const UInt64 MysqlUBigIntMin = UInt64.MinValue;
        protected const UInt64 MysqlUBigIntMax = UInt64.MaxValue;
        protected const Int64 MysqlBigIntMin = Int64.MinValue;
        protected const Int64 MysqlBigIntMax = Int64.MaxValue;

        protected const int OracleNumber1Min = -9;
        protected const int OracleNumber1Max = 9;
        protected const int OracleNumber5Min = -99999;
        protected const int OracleNumber5Max = 99999;
        protected const Int64 OracleNumber10Min = -9999999999;
        protected const Int64 OracleNumber10Max = 9999999999;
        protected readonly decimal OracleNumber19Min = Convert.ToDecimal("-9999999999999999999");
        protected const decimal OracleNumber19Max = 9999999999999999999;        

        protected const int SqlservTinyIntMin = Byte.MinValue;
        protected const int SqlservTinyIntMax = Byte.MaxValue;
        protected const int SqlservSmallIntMin = Int16.MinValue;
        protected const int SqlservSmallIntMax = Int16.MaxValue;
        protected const int SqlservIntMin = int.MinValue;
        protected const int SqlservIntMax = int.MaxValue;
        protected const Int64 SqlservBigIntMin = Int64.MinValue;
        protected const Int64 SqlservBigIntMax = Int64.MaxValue;
        protected readonly decimal SqlservSmallMoneyMin = Convert.ToDecimal("-214748.3648");
        protected readonly decimal SqlservSmallMoneyMax = Convert.ToDecimal("214748.3647");
        protected readonly decimal SqlservMoneyMin = Convert.ToDecimal("-922337203685477.5808");
        protected readonly decimal SqlservMoneyMax = Convert.ToDecimal("922337203685477.5807");

        internal static IEnumerable<TestCaseData> ConnectionData;
        internal static IEnumerable<TestCaseData> ConnectionAndIsolationData;
        internal static IEnumerable<TestCaseData> ConnectionNameAndData;
        protected Factory Factory = new Factory();

        protected TestBase()
        {
            ConnectionData = (from ConnectionStringSettings cs in ConfigurationManager.ConnectionStrings
                              select new TestCaseData(cs.ProviderName, cs.ConnectionString));

            ConnectionNameAndData = (from ConnectionStringSettings cs in ConfigurationManager.ConnectionStrings
                                     select new TestCaseData(cs.Name, cs.ProviderName, cs.ConnectionString));

            var list = new List<TestCaseData>();
            foreach (ConnectionStringSettings cs in ConfigurationManager.ConnectionStrings)
            {
                string readCommittedLiteral = null;
                string serializableLiteral = null;

                switch (cs.Name)
                {
                    case "Oracle":
                        readCommittedLiteral = @"READ COMMITTED";
                        serializableLiteral = @"SERIALIZABLE";
                        break;
                    case "MySql5.5.19":
                    case "MySql5.6.11":
                        readCommittedLiteral = @"READ-COMMITTED";
                        serializableLiteral = @"SERIALIZABLE";
                        break;
                    case "SqlServ2008":
                    case "SqlServCe":
                        readCommittedLiteral = @"ReadCommitted";
                        serializableLiteral = @"Serializable";
                        break;
                }

                var data = new TestCaseData(cs.ProviderName, cs.ConnectionString, readCommittedLiteral,
                                            serializableLiteral);
                list.Add(data);
            }
            ConnectionAndIsolationData = list;
        }

        protected void WithADbAccessRun(string providerName, string connectionString, Action<IDbAccess> testCodeToRun)
        {
            using (var db = Factory.CreateDbAccess(providerName, connectionString))
            {
                testCodeToRun(db);
            }
        }

        protected void WithACommandRun(string providerName, string connectionString, Action<ICommand> testCodeToRun)
        {
            WithADbAccessRun(providerName, connectionString,
                             db =>
                                 {
                                     using (var cmd = db.CreateCommand())
                                     {
                                         testCodeToRun(cmd);
                                     }
                                 });
        }

        protected void DeleteTransIsolationTestData(string providerName, string connectionString, Guid id)
        {
            WithACommandRun(providerName, connectionString,
                            cmd =>
                                {
                                    // sqlserver does not support truncate; deleting instead
                                    cmd.CommandText = @"delete from trans_isolvl where id = '" + id + "'";
                                    cmd.ExecuteNonQuery();
                                    cmd.CommandText = @"delete from trans_tab where id = '" + id + "'";
                                    cmd.ExecuteNonQuery();
                                });
        }

        protected int GetTransTableRowCount(string providerName, string connectionString, Guid id)
        {
            var count = 0;

            WithACommandRun(providerName, connectionString,
                            cmd =>
                                {
                                    cmd.CommandText = @"select count(*) from trans_tab where id = '" + id + "'";
                                    count = Convert.ToInt16(cmd.ExecuteScalar());
                                });
            return count;
        }

        protected string GetRecordedIsolationLevel(string providerName, string connectionString, Guid id)
        {
            string level = null;

            WithACommandRun(providerName, connectionString,
                            cmd =>
                                {
                                    cmd.CommandText = @"select isolationlevel from trans_isolvl where id = '" + id + "'";
                                    level = Convert.ToString(cmd.ExecuteScalar());
                                });
            return level;
        }
    }
}