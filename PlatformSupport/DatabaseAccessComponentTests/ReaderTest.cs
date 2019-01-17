using System;
using System.Globalization;
using NUnit.Framework;
using PlatformSupport.DatabaseAccess;

namespace PlatformSupport.DatabaseAccessComponentTests
{
    [TestFixture]
    public class ReaderTest : TestBase
    {
        private void WithinAReaderLoopRun(string providerName, string connectionString, string sqlCommand,
                                          int expectedRows, Action<IReader, int, int> testCodeToRun)
        {
            WithACommandRun(providerName, connectionString, cmd =>
                {
                    cmd.CommandText = sqlCommand;
                    using (var reader = cmd.ExecuteReader())
                    {
                        var rows = 0;
                        var fields = reader.FieldCount;
                        while (reader.AdvanceRow())
                        {
                            var fieldCount = 0;
                            while (fieldCount < fields)
                            {
                                testCodeToRun(reader, rows, fieldCount);
                                fieldCount++;
                            }
                            rows++;
                        }
                        Assert.AreEqual(expectedRows, rows);
                    }
                });
        }


        [TestCaseSource("ConnectionData")]
        public void AdvancesRow(string providerName, string connectionString)
        {
            WithACommandRun(providerName, connectionString,
                            cmd =>
                                {
                                    var id = 0;
                                    cmd.CommandText = @"select * from ten_row_tab";
                                    using (var reader = cmd.ExecuteReader())
                                    {
                                        while (reader.AdvanceRow())
                                        {
                                            id++;
                                        }
                                        Assert.AreEqual(10, id);
                                    }
                                });
        }

        [TestCaseSource("ConnectionData")]
        public void GetsFieldCount(string providerName, string connectionString)
        {
            WithACommandRun(providerName, connectionString,
                            cmd =>
                                {
                                    cmd.CommandText =
                                        @"select 'foo' as foo, 'bar' as bar, 'baz' as baz from scalar_test";
                                    using (var reader = cmd.ExecuteReader())
                                    {
                                        Assert.AreEqual(3, reader.FieldCount);
                                    }
                                });
        }

        [TestCaseSource("ConnectionData")]
        public void GetsColumnName(string providerName, string connectionString)
        {
            WithACommandRun(providerName, connectionString,
                            cmd =>
                                {
                                    cmd.CommandText =
                                        @"select 'foo' as foo, 'bar' as bar, 'baz' as baz from scalar_test";
                                    using (var reader = (Reader)cmd.ExecuteReader())
                                    {
                                        Assert.AreEqual(3, reader.FieldCount);
                                        Assert.That("foo", Is.EqualTo(reader.GetColumnName(0)).IgnoreCase);
                                        Assert.That("bar", Is.EqualTo(reader.GetColumnName(1)).IgnoreCase);
                                        Assert.That("baz", Is.EqualTo(reader.GetColumnName(2)).IgnoreCase);
                                    }
                                });
        }

        [TestCaseSource("ConnectionData")]
        public void GetsDbDateTimeAsStringFormat(string providerName, string connectionString)
        {
            WithACommandRun(providerName, connectionString,
                            cmd =>
                                {
                                    cmd.CommandText = @"select 'dummy' from scalar_test";
                                    using (var reader = cmd.ExecuteReader())
                                    {
                                        Assert.AreEqual(@"{0:0000}-{1:00}-{2:00} {3:00}:{4:00}:{5:00}.{6,9} {7}",
                                                        reader.DbDateTimeAsStringFormat);
                                    }
                                });
        }


        [TestCaseSource("ConnectionData")]
        public void GetBooleanGetsCorrectBooleanValue(string providerName, string connectionString)
        {
            const bool defaultValue = false;
            var getBoolSql = "";
            var expectedBool = new bool[,] { { } };

            switch (providerName)
            {
                case @"MySql.Data.MySqlClient":
                    getBoolSql = "select xbool, xbit from bools_tab order by record_description";
                    expectedBool = new [,] { { true, true }, { false, false }, { defaultValue, defaultValue } };
                    break;
                case @"Oracle.ManagedDataAccess.Client":
                    getBoolSql = "select xnum1 from bools_tab order by record_description";
                    expectedBool = new [,] { { true }, { false }, { false }, { defaultValue } };
                    break;
                case @"System.Data.SqlClient":
                    getBoolSql = "select xbit from bools_tab order by record_description";
                    expectedBool = new [,] { { true }, { false }, { defaultValue } };
                    break;
            }

            WithinAReaderLoopRun(providerName, connectionString, getBoolSql, expectedBool.GetLength(0),
                     (reader, rows, fieldCount) =>
                     {
                         var result = defaultValue;
                         reader.GetBoolean(((Reader)reader).GetColumnName(fieldCount), ref result);
                         Assert.AreEqual(expectedBool[rows, fieldCount], result);
                     });
        }


        [TestCaseSource("ConnectionData")]
        public void GetBooleanNullableGetsCorrectBooleanValue(string providerName, string connectionString)
        {
            var getBoolSql = "";
            var expectedBool = new bool?[,]{{}};

            switch (providerName)
            {
                case @"MySql.Data.MySqlClient":
                    getBoolSql = "select xbool, xbit from bools_tab order by record_description";
                    expectedBool = new bool?[,] { { true, true }, { false, false }, { null, null } };
                    break;
                case @"Oracle.ManagedDataAccess.Client":
                    getBoolSql = "select xnum1 from bools_tab order by record_description";
                    expectedBool = new bool?[,] { { true }, { false },{ false }, { null } };
                    break;
                case @"System.Data.SqlClient":
                    getBoolSql = "select xbit from bools_tab order by record_description";
                    expectedBool = new bool?[,] { { true }, { false }, { null } };
                    break;
            }

            WithinAReaderLoopRun(providerName, connectionString, getBoolSql, expectedBool.GetLength(0),
                     (reader, rows, fieldCount) =>
                     {
                         bool? result = null;
                         reader.GetBoolean(((Reader)reader).GetColumnName(fieldCount), ref result);
                         Assert.AreEqual(expectedBool[rows, fieldCount], result);
                     });
        }

        [TestCaseSource("ConnectionData")]
        public void GetFloatGetsCorrectApproximateNumbers(string providerName, string connectionString)
        {
            const int defaultValue = 99;
            var getFloatSql = "select xfloat, xfloat2 from approx_nums_tab order by record_description";
            var expectedFloat = new[,]
                {{float.MaxValue, float.MaxValue}, {float.MinValue, float.MinValue}, {defaultValue, defaultValue}};

            switch (providerName)
            {
                case @"MySql.Data.MySqlClient":
                    getFloatSql = "select xfloat from approx_nums_tab order by record_description";
                    expectedFloat = new[,] {{float.MaxValue}, {float.MinValue}, {defaultValue}};
                    break;
            }

            WithinAReaderLoopRun(providerName, connectionString, getFloatSql, expectedFloat.GetLength(0),
                                 (reader, rows, fieldCount) =>
                                     {
                                         var result = (float)defaultValue;
                                         const float delta = 0.00001e+38f;
                                         reader.GetFloat(((Reader)reader).GetColumnName(fieldCount), ref result);
                                         Assert.AreEqual(expectedFloat[rows, fieldCount], result, delta);
                                     });
        }

        [TestCaseSource("ConnectionData")]
        public void GetFloatNullableGetsCorrectApproximateNumbers(string providerName, string connectionString)
        {
            var getFloatSql = "select xfloat, xfloat2 from approx_nums_tab order by record_description";
            var expectedFloat = new float?[,] { { float.MaxValue, float.MaxValue }, { float.MinValue, float.MinValue }, { null, null } };

            switch (providerName)
            {
                case @"MySql.Data.MySqlClient":
                    getFloatSql = "select xfloat from approx_nums_tab order by record_description";
                    expectedFloat = new float?[,] { { float.MaxValue }, { float.MinValue }, { null } };
                    break;
            }

            WithinAReaderLoopRun(providerName, connectionString, getFloatSql, expectedFloat.GetLength(0),
                                 (reader, rows, fieldCount) =>
                                 {
                                     float? result = null;
                                     const float delta = 0.00001e+38f;
                                     reader.GetFloat(((Reader)reader).GetColumnName(fieldCount), ref result);
                                     Assert.That(result, Is.EqualTo(expectedFloat[rows, fieldCount]).Within(delta));
                                 });
        }

        [TestCaseSource("ConnectionData")]
        public void GetDoubleGetsCorrectApproximateNumbers(string providerName, string connectionString)
        {
            const int defaultValue = 99;
            var getDoubleSql = "select xfloat, xfloat2, xdouble from approx_nums_tab order by record_description";
            var expectedDouble = new[,]
                {
                    {float.MaxValue, float.MaxValue, double.MaxValue},
                    {float.MinValue, float.MinValue, double.MinValue},
                    {defaultValue, defaultValue, defaultValue}
                };

            switch (providerName)
            {
                case @"MySql.Data.MySqlClient":
                    getDoubleSql = "select xfloat, xdouble from approx_nums_tab order by record_description";
                    expectedDouble = new[,]
                        {
                            {float.MaxValue, double.MaxValue}, {float.MinValue, double.MinValue},
                            {defaultValue, defaultValue}
                        };
                    break;
            }

            WithinAReaderLoopRun(providerName, connectionString, getDoubleSql, expectedDouble.GetLength(0),
                                 (reader, rows, fieldCount) =>
                                     {
                                         var result = (double)defaultValue;
                                         const double delta = 0.00000000000000001e+308d;
                                         reader.GetDouble(((Reader)reader).GetColumnName(fieldCount), ref result);
                                         Assert.AreEqual(expectedDouble[rows, fieldCount], result, delta);
                                     });
        }


        [TestCaseSource("ConnectionData")]
        public void GetDoubleNullableGetsCorrectApproximateNumbers(string providerName, string connectionString)
        {
            var getDoubleSql = "select xfloat, xfloat2, xdouble from approx_nums_tab order by record_description";
            var expectedDouble = new double?[,]
                {
                    {float.MaxValue, float.MaxValue, double.MaxValue},
                    {float.MinValue, float.MinValue, double.MinValue},
                    {null, null, null}
                };

            switch (providerName)
            {
                case @"MySql.Data.MySqlClient":
                    getDoubleSql = "select xfloat, xdouble from approx_nums_tab order by record_description";
                    expectedDouble = new double?[,]
                        {
                            {float.MaxValue, double.MaxValue}, {float.MinValue, double.MinValue},
                            {null, null}
                        };
                    break;
            }

            WithinAReaderLoopRun(providerName, connectionString, getDoubleSql, expectedDouble.GetLength(0),
                                 (reader, rows, fieldCount) =>
                                 {
                                     double? result = null;
                                     const double delta = 0.00000000000000001e+308d;
                                     reader.GetDouble(((Reader)reader).GetColumnName(fieldCount), ref result);
                                     Assert.That(result, Is.EqualTo(expectedDouble[rows, fieldCount]).Within(delta));
                                 });
        }


        [TestCaseSource("ConnectionData")]
        public void GetByteGetsCorrectExactNumbers(string providerName, string connectionString)
        {
            const int defaultValue = 99;
            var getByteSql = "";
            var expectedByte = new byte[,] {{}};

            switch (providerName)
            {
                case @"MySql.Data.MySqlClient":
                    getByteSql = @"select xutinyint from exact_nums_tab order by record_description";
                    expectedByte = new byte[,] {{MysqlUTinyIntMax}, {MysqlUTinyIntMin}, {(byte)defaultValue}};
                    break;
                case @"Oracle.ManagedDataAccess.Client":
                    getByteSql = "select null from exact_nums_tab order by record_description";
                    expectedByte = new[,] {{(byte)defaultValue}, {(byte)defaultValue}, {(byte)defaultValue}};
                    break;
                case @"System.Data.SqlClient":
                    getByteSql = @"select xtinyint from exact_nums_tab order by record_description";
                    expectedByte = new byte[,] {{SqlservTinyIntMax}, {SqlservTinyIntMin}, {(byte)defaultValue}};
                    break;
            }

            WithinAReaderLoopRun(providerName, connectionString, getByteSql, expectedByte.GetLength(0),
                                 (reader, rows, fieldCount) =>
                                     {
                                         var result = (byte)defaultValue;
                                         reader.GetByte(((Reader)reader).GetColumnName(fieldCount), ref result);
                                         Assert.AreEqual(expectedByte[rows, fieldCount], result);
                                     });
        }

        [TestCaseSource("ConnectionData")]
        public void GetByteNullableGetsCorrectExactNumbers(string providerName, string connectionString)
        {
            var getByteSql = "";
            var expectedByte = new byte?[,] { { } };

            switch (providerName)
            {
                case @"MySql.Data.MySqlClient":
                    getByteSql = @"select xutinyint from exact_nums_tab order by record_description";
                    expectedByte = new byte?[,] { { MysqlUTinyIntMax }, { MysqlUTinyIntMin }, { null } };
                    break;
                case @"Oracle.ManagedDataAccess.Client":
                    getByteSql = "select null from exact_nums_tab order by record_description";
                    expectedByte = new byte?[,] { { null }, { null }, { null } };
                    break;
                case @"System.Data.SqlClient":
                    getByteSql = @"select xtinyint from exact_nums_tab order by record_description";
                    expectedByte = new byte?[,] { { SqlservTinyIntMax }, { SqlservTinyIntMin }, { null } };
                    break;
            }

            WithinAReaderLoopRun(providerName, connectionString, getByteSql, expectedByte.GetLength(0),
                                 (reader, rows, fieldCount) =>
                                 {
                                     byte? result = null;
                                     reader.GetByte(((Reader)reader).GetColumnName(fieldCount), ref result);
                                     Assert.AreEqual(expectedByte[rows, fieldCount], result);
                                 });
        }

        [TestCaseSource("ConnectionData")]
        public void GetInt16GetsCorrectExactNumbers(string providerName, string connectionString)
        {
            const int defaultValue = 99;
            var getInt16Sql = "";
            var expectedInt16 = new Int16[,] {{}};

            switch (providerName)
            {
                case @"MySql.Data.MySqlClient":
                    getInt16Sql =
                        @"select xutinyint, xtinyint, xsmallint from exact_nums_tab order by record_description";
                    expectedInt16 = new Int16[,]
                        {
                            {MysqlUTinyIntMax, MysqlTinyIntMax, MysqlSmallIntMax},
                            {MysqlUTinyIntMin, MysqlTinyIntMin, MysqlSmallIntMin},
                            {(short)defaultValue, (short)defaultValue, (short)defaultValue}
                        };
                    break;
                case @"Oracle.ManagedDataAccess.Client":
                    getInt16Sql = @"select xnum1 from exact_nums_tab order by record_description";
                    expectedInt16 = new Int16[,] {{OracleNumber1Max}, {OracleNumber1Min}, {defaultValue}};
                    break;
                case @"System.Data.SqlClient":
                    getInt16Sql = @"select xtinyint, xsmallint from exact_nums_tab order by record_description";
                    expectedInt16 = new Int16[,]
                        {
                            {SqlservTinyIntMax, SqlservSmallIntMax}, {SqlservTinyIntMin, SqlservSmallIntMin},
                            {defaultValue, defaultValue}
                        };
                    break;
            }

            WithinAReaderLoopRun(providerName, connectionString, getInt16Sql, expectedInt16.GetLength(0),
                                 (reader, rows, fieldCount) =>
                                     {
                                         var result = (Int16)defaultValue;
                                         reader.GetInt16(((Reader)reader).GetColumnName(fieldCount), ref result);
                                         Assert.AreEqual(expectedInt16[rows, fieldCount], result);
                                     });
        }

        [TestCaseSource("ConnectionData")]
        public void GetInt16NullableGetsCorrectExactNumbers(string providerName, string connectionString)
        {
            var getInt16Sql = "";
            var expectedInt16 = new Int16?[,] { { } };

            switch (providerName)
            {
                case @"MySql.Data.MySqlClient":
                    getInt16Sql =
                        @"select xutinyint, xtinyint, xsmallint from exact_nums_tab order by record_description";
                    expectedInt16 = new Int16?[,]
                        {
                            {MysqlUTinyIntMax, MysqlTinyIntMax, MysqlSmallIntMax},
                            {MysqlUTinyIntMin, MysqlTinyIntMin, MysqlSmallIntMin},
                            {null, null, null}
                        };
                    break;
                case @"Oracle.ManagedDataAccess.Client":
                    getInt16Sql = @"select xnum1 from exact_nums_tab order by record_description";
                    expectedInt16 = new Int16?[,] { { OracleNumber1Max }, { OracleNumber1Min }, { null } };
                    break;
                case @"System.Data.SqlClient":
                    getInt16Sql = @"select xtinyint, xsmallint from exact_nums_tab order by record_description";
                    expectedInt16 = new Int16?[,]
                        {
                            {SqlservTinyIntMax, SqlservSmallIntMax}, {SqlservTinyIntMin, SqlservSmallIntMin},
                            {null, null}
                        };
                    break;
            }

            WithinAReaderLoopRun(providerName, connectionString, getInt16Sql, expectedInt16.GetLength(0),
                                 (reader, rows, fieldCount) =>
                                 {
                                     Int16? result = null;
                                     reader.GetInt16(((Reader)reader).GetColumnName(fieldCount), ref result);
                                     Assert.AreEqual(expectedInt16[rows, fieldCount], result);
                                 });
        }

        [TestCaseSource("ConnectionData")]
        public void GetIntGetsCorrectExactNumbers(string providerName, string connectionString)
        {
            const int defaultValue = 99;
            var getIntSql = "";
            var expectedInt = new int[,] {{}};

            switch (providerName)
            {
                case @"MySql.Data.MySqlClient":
                    getIntSql =
                        @"select xutinyint, xtinyint, xusmallint, xsmallint, xumediumint, xmediumint, xint from exact_nums_tab order by record_description";
                    expectedInt = new[,]
                        {
                            {
                                MysqlUTinyIntMax, MysqlTinyIntMax, MysqlUSmallIntMax, MysqlSmallIntMax,
                                MysqlUMediumIntMax,
                                MysqlMediumIntMax, MysqlIntMax
                            },
                            {
                                MysqlUTinyIntMin, MysqlTinyIntMin, MysqlUSmallIntMin, MysqlSmallIntMin,
                                MysqlUMediumIntMin,
                                MysqlMediumIntMin, MysqlIntMin
                            },
                            {
                                defaultValue, defaultValue, defaultValue, defaultValue, defaultValue, defaultValue,
                                defaultValue
                            }
                        };
                    break;
                case @"Oracle.ManagedDataAccess.Client":
                    getIntSql = @"select xnum1, xnum5 from exact_nums_tab order by record_description";
                    expectedInt = new[,]
                        {
                            {OracleNumber1Max, OracleNumber5Max}, {OracleNumber1Min, OracleNumber5Min},
                            {defaultValue, defaultValue}
                        };
                    break;
                case @"System.Data.SqlClient":
                    getIntSql = @"select xtinyint, xsmallint, xint from exact_nums_tab order by record_description";
                    expectedInt = new[,]
                        {
                            {SqlservTinyIntMax, SqlservSmallIntMax, SqlservIntMax},
                            {SqlservTinyIntMin, SqlservSmallIntMin, SqlservIntMin},
                            {defaultValue, defaultValue, defaultValue}
                        };
                    break;
            }

            WithinAReaderLoopRun(providerName, connectionString, getIntSql, expectedInt.GetLength(0),
                                 (reader, rows, fieldCount) =>
                                     {
                                         var result = defaultValue;
                                         reader.GetInt(((Reader)reader).GetColumnName(fieldCount), ref result);
                                         Assert.AreEqual(expectedInt[rows, fieldCount], result);
                                     });
        }

        [TestCaseSource("ConnectionData")]
        public void GetIntNullableGetsCorrectExactNumbers(string providerName, string connectionString)
        {
            var getIntSql = "";
            var expectedInt = new int?[,] { { } };

            switch (providerName)
            {
                case @"MySql.Data.MySqlClient":
                    getIntSql =
                        @"select xutinyint, xtinyint, xusmallint, xsmallint, xumediumint, xmediumint, xint from exact_nums_tab order by record_description";
                    expectedInt = new int?[,]
                        {
                            {
                                MysqlUTinyIntMax, MysqlTinyIntMax, MysqlUSmallIntMax, MysqlSmallIntMax,
                                MysqlUMediumIntMax,
                                MysqlMediumIntMax, MysqlIntMax
                            },
                            {
                                MysqlUTinyIntMin, MysqlTinyIntMin, MysqlUSmallIntMin, MysqlSmallIntMin,
                                MysqlUMediumIntMin,
                                MysqlMediumIntMin, MysqlIntMin
                            },
                            {
                                null, null, null, null, null, null,null
                            }
                        };
                    break;
                case @"Oracle.ManagedDataAccess.Client":
                    getIntSql = @"select xnum1, xnum5 from exact_nums_tab order by record_description";
                    expectedInt = new int?[,]
                        {
                            {OracleNumber1Max, OracleNumber5Max}, {OracleNumber1Min, OracleNumber5Min},
                            {null, null}
                        };
                    break;
                case @"System.Data.SqlClient":
                    getIntSql = @"select xtinyint, xsmallint, xint from exact_nums_tab order by record_description";
                    expectedInt = new int?[,]
                        {
                            {SqlservTinyIntMax, SqlservSmallIntMax, SqlservIntMax},
                            {SqlservTinyIntMin, SqlservSmallIntMin, SqlservIntMin},
                            {null, null, null}
                        };
                    break;
            }

            WithinAReaderLoopRun(providerName, connectionString, getIntSql, expectedInt.GetLength(0),
                                 (reader, rows, fieldCount) =>
                                 {
                                     int? result = null;
                                     reader.GetInt(((Reader)reader).GetColumnName(fieldCount), ref result);
                                     Assert.AreEqual(expectedInt[rows, fieldCount], result);
                                 });
        }

        [TestCaseSource("ConnectionData")]
        public void GetInt64GetsCorrectExactNumbers(string providerName, string connectionString)
        {
            const int defaultValue = 99;
            var getInt64Sql = "";
            var expectedInt64 = new Int64[,] {{}};

            switch (providerName)
            {
                case @"MySql.Data.MySqlClient":
                    getInt64Sql =
                        @"select xutinyint, xtinyint, xusmallint, xsmallint, xumediumint, xmediumint, xuint, xint, xbigint from exact_nums_tab order by record_description";
                    expectedInt64 = new[,]
                        {
                            {
                                MysqlUTinyIntMax, MysqlTinyIntMax, MysqlUSmallIntMax, MysqlSmallIntMax,
                                MysqlUMediumIntMax,
                                MysqlMediumIntMax, MysqlUIntMax, MysqlIntMax, MysqlBigIntMax
                            },
                            {
                                MysqlUTinyIntMin, MysqlTinyIntMin, MysqlUSmallIntMin, MysqlSmallIntMin,
                                MysqlUMediumIntMin,
                                MysqlMediumIntMin, MysqlUIntMin, MysqlIntMin, MysqlBigIntMin
                            },
                            {
                                defaultValue, defaultValue, defaultValue, defaultValue, defaultValue, defaultValue,
                                defaultValue, defaultValue, defaultValue
                            }
                        };
                    break;
                case @"Oracle.ManagedDataAccess.Client":
                    getInt64Sql = @"select xnum1, xnum5, xnum10 from exact_nums_tab order by record_description";
                    expectedInt64 = new[,]
                        {
                            {OracleNumber1Max, OracleNumber5Max, OracleNumber10Max},
                            {OracleNumber1Min, OracleNumber5Min, OracleNumber10Min},
                            {defaultValue, defaultValue, defaultValue}
                        };
                    break;
                case @"System.Data.SqlClient":
                    getInt64Sql =
                        @"select xtinyint, xsmallint, xint, xbigint from exact_nums_tab order by record_description";
                    expectedInt64 = new[,]
                        {
                            {SqlservTinyIntMax, SqlservSmallIntMax, SqlservIntMax, SqlservBigIntMax},
                            {SqlservTinyIntMin, SqlservSmallIntMin, SqlservIntMin, SqlservBigIntMin},
                            {defaultValue, defaultValue, defaultValue, defaultValue}
                        };
                    break;
            }

            WithinAReaderLoopRun(providerName, connectionString, getInt64Sql, expectedInt64.GetLength(0),
                                 (reader, rows, fieldCount) =>
                                     {
                                         var result = (Int64)defaultValue;
                                         reader.GetInt64(((Reader)reader).GetColumnName(fieldCount), ref result);
                                         Assert.AreEqual(expectedInt64[rows, fieldCount], result);
                                     });
        }

        [TestCaseSource("ConnectionData")]
        public void GetInt64NullableGetsCorrectExactNumbers(string providerName, string connectionString)
        {
            var getInt64Sql = "";
            var expectedInt64 = new Int64?[,] { { } };

            switch (providerName)
            {
                case @"MySql.Data.MySqlClient":
                    getInt64Sql =
                        @"select xutinyint, xtinyint, xusmallint, xsmallint, xumediumint, xmediumint, xuint, xint, xbigint from exact_nums_tab order by record_description";
                    expectedInt64 = new Int64?[,]
                        {
                            {
                                MysqlUTinyIntMax, MysqlTinyIntMax, MysqlUSmallIntMax, MysqlSmallIntMax,
                                MysqlUMediumIntMax,
                                MysqlMediumIntMax, MysqlUIntMax, MysqlIntMax, MysqlBigIntMax
                            },
                            {
                                MysqlUTinyIntMin, MysqlTinyIntMin, MysqlUSmallIntMin, MysqlSmallIntMin,
                                MysqlUMediumIntMin,
                                MysqlMediumIntMin, MysqlUIntMin, MysqlIntMin, MysqlBigIntMin
                            },
                            {
                                null, null, null, null, null, null,null, null, null
                            }
                        };
                    break;
                case @"Oracle.ManagedDataAccess.Client":
                    getInt64Sql = @"select xnum1, xnum5, xnum10 from exact_nums_tab order by record_description";
                    expectedInt64 = new Int64?[,]
                        {
                            {OracleNumber1Max, OracleNumber5Max, OracleNumber10Max},
                            {OracleNumber1Min, OracleNumber5Min, OracleNumber10Min},
                            {null, null, null}
                        };
                    break;
                case @"System.Data.SqlClient":
                    getInt64Sql =
                        @"select xtinyint, xsmallint, xint, xbigint from exact_nums_tab order by record_description";
                    expectedInt64 = new Int64?[,]
                        {
                            {SqlservTinyIntMax, SqlservSmallIntMax, SqlservIntMax, SqlservBigIntMax},
                            {SqlservTinyIntMin, SqlservSmallIntMin, SqlservIntMin, SqlservBigIntMin},
                            {null, null, null, null}
                        };
                    break;
            }


            WithinAReaderLoopRun(providerName, connectionString, getInt64Sql, expectedInt64.GetLength(0),
                                 (reader, rows, fieldCount) =>
                                 {
                                     Int64? result = null;
                                     reader.GetInt64(((Reader)reader).GetColumnName(fieldCount), ref result);
                                     Assert.AreEqual(expectedInt64[rows, fieldCount], result);
                                 });
        }

        [TestCaseSource("ConnectionData")]
        public void GetDecimalGetsCorrectExactNumbers(string providerName, string connectionString)
        {
            const int defaultValue = 99;
            var getDecimalSql = "";
            var expectedDecimal = new decimal[,] {{}};

            switch (providerName)
            {
                case @"MySql.Data.MySqlClient":
                    getDecimalSql =
                        @"select xutinyint, xtinyint, xusmallint, xsmallint, xumediumint, xmediumint, xuint, xint, xubigint, xbigint from exact_nums_tab order by record_description";
                    expectedDecimal = new decimal[,]
                        {
                            {
                                MysqlUTinyIntMax, MysqlTinyIntMax, MysqlUSmallIntMax, MysqlSmallIntMax,
                                MysqlUMediumIntMax,
                                MysqlMediumIntMax, MysqlUIntMax, MysqlIntMax, MysqlUBigIntMax, MysqlBigIntMax
                            },
                            {
                                MysqlUTinyIntMin, MysqlTinyIntMin, MysqlUSmallIntMin, MysqlSmallIntMin,
                                MysqlUMediumIntMin,
                                MysqlMediumIntMin, MysqlUIntMin, MysqlIntMin, MysqlUBigIntMin, MysqlBigIntMin
                            },
                            {
                                defaultValue, defaultValue, defaultValue, defaultValue, defaultValue, defaultValue,
                                defaultValue, defaultValue, defaultValue, defaultValue
                            }
                        };
                    break;
                case @"Oracle.ManagedDataAccess.Client":
                    getDecimalSql =
                        @"select xnum1, xnum5, xnum10, xnum19 from exact_nums_tab order by record_description";
                    expectedDecimal = new[,]
                        {
                            {OracleNumber1Max, OracleNumber5Max, OracleNumber10Max, OracleNumber19Max},
                            {OracleNumber1Min, OracleNumber5Min, OracleNumber10Min, OracleNumber19Min},
                            {defaultValue, defaultValue, defaultValue, defaultValue}
                        };
                    break;
                case @"System.Data.SqlClient":
                    getDecimalSql =
                        @"select xtinyint, xsmallint, xint, xbigint, xsmallmoney, xmoney from exact_nums_tab order by record_description";
                    expectedDecimal = new[,]
                        {
                            {
                                SqlservTinyIntMax, SqlservSmallIntMax, SqlservIntMax, SqlservBigIntMax,
                                SqlservSmallMoneyMax, SqlservMoneyMax
                            },
                            {
                                SqlservTinyIntMin, SqlservSmallIntMin, SqlservIntMin, SqlservBigIntMin,
                                SqlservSmallMoneyMin, SqlservMoneyMin
                            },
                            {defaultValue, defaultValue, defaultValue, defaultValue, defaultValue, defaultValue}
                        };
                    break;
            }

            WithinAReaderLoopRun(providerName, connectionString, getDecimalSql, expectedDecimal.GetLength(0),
                                 (reader, rows, fieldCount) =>
                                     {
                                         var result = (decimal)defaultValue;
                                         reader.GetDecimal(((Reader)reader).GetColumnName(fieldCount), ref result);
                                         Assert.AreEqual(expectedDecimal[rows, fieldCount], result);
                                     });
        }

        [TestCaseSource("ConnectionData")]
        public void GetDecimalNullableGetsCorrectExactNumbers(string providerName, string connectionString)
        {
            var getDecimalSql = "";
            var expectedDecimal = new decimal?[,] { { } };

            switch (providerName)
            {
                case @"MySql.Data.MySqlClient":
                    getDecimalSql =
                        @"select xutinyint, xtinyint, xusmallint, xsmallint, xumediumint, xmediumint, xuint, xint, xubigint, xbigint from exact_nums_tab order by record_description";
                    expectedDecimal = new decimal?[,]
                        {
                            {
                                MysqlUTinyIntMax, MysqlTinyIntMax, MysqlUSmallIntMax, MysqlSmallIntMax,
                                MysqlUMediumIntMax,
                                MysqlMediumIntMax, MysqlUIntMax, MysqlIntMax, MysqlUBigIntMax, MysqlBigIntMax
                            },
                            {
                                MysqlUTinyIntMin, MysqlTinyIntMin, MysqlUSmallIntMin, MysqlSmallIntMin,
                                MysqlUMediumIntMin,
                                MysqlMediumIntMin, MysqlUIntMin, MysqlIntMin, MysqlUBigIntMin, MysqlBigIntMin
                            },
                            {
                                null, null, null, null, null, null,null, null, null, null
                            }
                        };
                    break;
                case @"Oracle.ManagedDataAccess.Client":
                    getDecimalSql =
                        @"select xnum1, xnum5, xnum10, xnum19 from exact_nums_tab order by record_description";
                    expectedDecimal = new decimal?[,]
                        {
                            {OracleNumber1Max, OracleNumber5Max, OracleNumber10Max, OracleNumber19Max},
                            {OracleNumber1Min, OracleNumber5Min, OracleNumber10Min, OracleNumber19Min},
                            {null, null, null, null}
                        };
                    break;
                case @"System.Data.SqlClient":
                    getDecimalSql =
                        @"select xtinyint, xsmallint, xint, xbigint, xsmallmoney, xmoney from exact_nums_tab order by record_description";
                    expectedDecimal = new decimal?[,]
                        {
                            {
                                SqlservTinyIntMax, SqlservSmallIntMax, SqlservIntMax, SqlservBigIntMax,
                                SqlservSmallMoneyMax, SqlservMoneyMax
                            },
                            {
                                SqlservTinyIntMin, SqlservSmallIntMin, SqlservIntMin, SqlservBigIntMin,
                                SqlservSmallMoneyMin, SqlservMoneyMin
                            },
                            {null, null, null, null, null, null}
                        };
                    break;
            }

            WithinAReaderLoopRun(providerName, connectionString, getDecimalSql, expectedDecimal.GetLength(0),
                                 (reader, rows, fieldCount) =>
                                 {
                                     decimal? result = null;
                                     reader.GetDecimal(((Reader)reader).GetColumnName(fieldCount), ref result);
                                     Assert.AreEqual(expectedDecimal[rows, fieldCount], result);
                                 });
        }

        [TestCaseSource("ConnectionData")]
        public void GetDecimalThrowsOverflowExceptionForNumberOutsideDotNetDecimalRange(string providerName,
                                                                                        string connectionString)
        {
            const int defaultValue = 99;
            var getDecimalSql = "";
            const int expectedRows = 2;

            switch (providerName)
            {
                case @"MySql.Data.MySqlClient":
                    getDecimalSql =
                        @"select xdecimal from exact_nums_tab where record_description not like '%null' order by record_description";
                    break;
                case @"Oracle.ManagedDataAccess.Client":
                    getDecimalSql =
                        @"select xfloat, xdecimal, xdecimal2 from exact_nums_tab where record_description not like '%null' order by record_description";
                    break;
                case @"System.Data.SqlClient":
                    getDecimalSql =
                        @"select xdecimal, xdecimal2 from exact_nums_tab where record_description not like '%null' order by record_description";
                    break;
            }

            WithinAReaderLoopRun(providerName, connectionString, getDecimalSql, expectedRows,
                                 (reader, rows, fieldCount) => Assert.Throws(Is.TypeOf<OverflowException>(), () =>
                                     {
                                         var result = (decimal)defaultValue;
                                         reader.GetDecimal(((Reader)reader).GetColumnName(fieldCount), ref result);
                                     }));

            WithinAReaderLoopRun(providerName, connectionString, getDecimalSql, expectedRows,
                                 (reader, rows, fieldCount) => Assert.Throws(Is.TypeOf<OverflowException>(), () =>
                                 {
                                     decimal? result = null;
                                     reader.GetDecimal(((Reader)reader).GetColumnName(fieldCount), ref result);
                                 }));
        }


        [TestCaseSource("ConnectionData")]
        public void GetDecimalStringGetsCorrectNumberAsString(string providerName, string connectionString)
        {
            const int defaultValue = 99;
            var getDecimalStringSql = "";
            var expectedDecimalString = new string[,] {{}};

            switch (providerName)
            {
                case @"MySql.Data.MySqlClient":
                    getDecimalStringSql = @"select xdecimal from exact_nums_tab order by record_description";
                    expectedDecimalString = new[,]
                        {
                            {"99999999999999999999999999999999999.999999999999999999999999999999"},
                            {"-99999999999999999999999999999999999.999999999999999999999999999999"},
                            {defaultValue.ToString(CultureInfo.InvariantCulture)}
                        };
                    break;
                case @"Oracle.ManagedDataAccess.Client":
                    getDecimalStringSql =
                        @"select xfloat, xdecimal, xdecimal2 from exact_nums_tab order by record_description";
                    expectedDecimalString = new[,]
                        {
                            {
                                "99999999999999999999999999999999999999", "99999999999999999999999999999999999999",
                                ".99999999999999999999999999999999999999"
                            },
                            {
                                "-9999999999999999999999999999999999999.9", "-99999999999999999999999999999999999999",
                                "-.99999999999999999999999999999999999999"
                            },
                            {
                                defaultValue.ToString(CultureInfo.InvariantCulture),
                                defaultValue.ToString(CultureInfo.InvariantCulture),
                                defaultValue.ToString(CultureInfo.InvariantCulture)
                            }
                        };
                    break;
                case @"System.Data.SqlClient":
                    getDecimalStringSql = @"select xdecimal, xdecimal2 from exact_nums_tab order by record_description";
                    expectedDecimalString = new[,]
                        {
                            {"99999999999999999999999999999999999999", "0.99999999999999999999999999999999999999"},
                            {"-99999999999999999999999999999999999999", "-0.99999999999999999999999999999999999999"},
                            {
                                defaultValue.ToString(CultureInfo.InvariantCulture),
                                defaultValue.ToString(CultureInfo.InvariantCulture)
                            }
                        };
                    break;
            }

            WithinAReaderLoopRun(providerName, connectionString, getDecimalStringSql, expectedDecimalString.GetLength(0),
                                 (reader, rows, fieldCount) =>
                                     {
                                         var result = defaultValue.ToString(CultureInfo.InvariantCulture);
                                         reader.GetDecimalString(((Reader)reader).GetColumnName(fieldCount), ref result);
                                         Assert.AreEqual(expectedDecimalString[rows, fieldCount], result);
                                     });
        }


        [TestCaseSource("ConnectionNameAndData")]
        public void GetDateTimeGetsCorrectDateTimeAtHighestDotNetResolution(string configName, string providerName,
                                                                            string connectionString)
        {
            var defaultValue = DateTime.Now;
            var getDateTimeSql = "";
            var expectedDateTimes = new DateTime[,] {{}};

            const string mysqlGetDateTimeSql =
                @"select xdate, xdatetime, xtimestamp from datetimes_tab where record_description not like '%invalid' order by record_description";

            switch (configName)
            {
                case @"MySql5.5.19":
                    getDateTimeSql = mysqlGetDateTimeSql;
                    expectedDateTimes = new[,]
                        {
                            {
                                DateTime.ParseExact(@"9999-12-31", "yyyy-MM-dd", CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"9999-12-31 23:59:59", "yyyy-MM-dd HH:mm:ss",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2038-01-18 19:14:07", "yyyy-MM-dd HH:mm:ss",
                                                    CultureInfo.InvariantCulture)
                            },
                            {
                                DateTime.ParseExact(@"1000-01-01", "yyyy-MM-dd", CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"1000-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"1970-01-01 00:00:01", "yyyy-MM-dd HH:mm:ss",
                                                    CultureInfo.InvariantCulture)
                            },
                            {
                                DateTime.ParseExact(@"2013-01-01", "yyyy-MM-dd", CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2013-01-01 01:01:01", "yyyy-MM-dd HH:mm:ss",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2013-01-01 01:01:01", "yyyy-MM-dd HH:mm:ss",
                                                    CultureInfo.InvariantCulture)
                            },
                            {defaultValue, defaultValue, defaultValue}
                        };
                    break;
                case @"MySql5.6.11":
                    getDateTimeSql = mysqlGetDateTimeSql;
                    expectedDateTimes = new[,]
                        {
                            {
                                DateTime.ParseExact(@"9999-12-31", "yyyy-MM-dd", CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"9999-12-31 23:59:59.999999", "yyyy-MM-dd HH:mm:ss.ffffff",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2038-01-18 19:14:07.999999", "yyyy-MM-dd HH:mm:ss.ffffff",
                                                    CultureInfo.InvariantCulture)
                            },
                            {
                                DateTime.ParseExact(@"1000-01-01", "yyyy-MM-dd", CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"1000-01-01 00:00:00.000000", "yyyy-MM-dd HH:mm:ss.ffffff",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"1970-01-01 00:00:01.000000", "yyyy-MM-dd HH:mm:ss.ffffff",
                                                    CultureInfo.InvariantCulture)
                            },
                            {
                                DateTime.ParseExact(@"2013-01-01", "yyyy-MM-dd", CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2013-01-01 01:01:01.123456", "yyyy-MM-dd HH:mm:ss.ffffff",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2013-01-01 01:01:01.123456", "yyyy-MM-dd HH:mm:ss.ffffff",
                                                    CultureInfo.InvariantCulture)
                            },
                            {defaultValue, defaultValue, defaultValue}
                        };
                    break;
                case @"Oracle":
                    getDateTimeSql =
                        @"select xdate,xtimestamp,xtimestamptz,xtimestampltz from datetimes_tab where record_description not like '%min' order by record_description";
                    expectedDateTimes = new[,]
                        {
                            {
                                DateTime.ParseExact(@"9999-12-31 23:59:59", "yyyy-MM-dd HH:mm:ss",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"9999-12-31 23:59:59.9999999", "yyyy-MM-dd HH:mm:ss.fffffff",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"9999-12-31 23:59:59.9999999 +14:00",
                                                    "yyyy-MM-dd HH:mm:ss.fffffff zzz", CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"9999-12-30 00:59:59.9999999",
                                                    "yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture)
                            },
                            {
                                DateTime.ParseExact(@"2013-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2013-01-01 01:01:01.1234567", "yyyy-MM-dd HH:mm:ss.fffffff",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2013-01-01 01:01:01.1234567 -08:00",
                                                    "yyyy-MM-dd HH:mm:ss.fffffff zzz", CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2013-01-01 02:01:01.1234567",
                                                    "yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture)
                            },
                            {defaultValue, defaultValue, defaultValue, defaultValue}
                        };
                    break;
                case @"SqlServ2008":
                    getDateTimeSql =
                        @"select xdate, xsmalldatetime, xdatetime, xdatetime2, xdatetimeoffset from datetimes_tab order by record_description";
                    expectedDateTimes = new[,]
                        {
                            {
                                DateTime.ParseExact(@"9999-12-31", "yyyy-MM-dd", CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2079-06-06 23:59:00", "yyyy-MM-dd HH:mm:ss",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"9999-12-31 23:59:59.997", "yyyy-MM-dd HH:mm:ss.fff",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"9999-12-31 23:59:59.9999999", "yyyy-MM-dd HH:mm:ss.fffffff",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"9999-12-31 23:59:59.9999999 +14:00",
                                                    "yyyy-MM-dd HH:mm:ss.fffffff zzz", CultureInfo.InvariantCulture)
                            },
                            {
                                DateTime.ParseExact(@"0001-01-01", "yyyy-MM-dd", CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"1900-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"1753-01-01 00:00:00.000", "yyyy-MM-dd HH:mm:ss.fff",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"0001-01-01 00:00:00.0000000", "yyyy-MM-dd HH:mm:ss.fffffff",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"0001-01-01 00:00:00.0000000 -14:00",
                                                    "yyyy-MM-dd HH:mm:ss.fffffff zzz", CultureInfo.InvariantCulture)
                            },
                            {
                                DateTime.ParseExact(@"2013-01-01", "yyyy-MM-dd", CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2013-01-01 01:01:00", "yyyy-MM-dd HH:mm:ss",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2013-01-01 01:01:01.123", "yyyy-MM-dd HH:mm:ss.fff",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2013-01-01 01:01:01.1234567", "yyyy-MM-dd HH:mm:ss.fffffff",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2013-01-01 01:01:01.1234567 -08:00",
                                                    "yyyy-MM-dd HH:mm:ss.fffffff zzz", CultureInfo.InvariantCulture)
                            },
                            {defaultValue, defaultValue, defaultValue, defaultValue, defaultValue}
                        };
                    break;
            }

            WithinAReaderLoopRun(providerName, connectionString, getDateTimeSql, expectedDateTimes.GetLength(0),
                                 (reader, rows, fieldCount) =>
                                     {
                                         var result = defaultValue;
                                         reader.GetDateTime(((Reader)reader).GetColumnName(fieldCount), ref result);
                                         Assert.AreEqual(expectedDateTimes[rows, fieldCount], result);
                                     });
        }

        [TestCaseSource("ConnectionNameAndData")]
        public void GetDateTimeNullableGetsCorrectDateTimeAtHighestDotNetResolution(string configName, string providerName,
                                                                            string connectionString)
        {
            var getDateTimeSql = "";
            var expectedDateTimes = new DateTime?[,] { { } };

            const string mysqlGetDateTimeSql =
                @"select xdate, xdatetime, xtimestamp from datetimes_tab where record_description not like '%invalid' order by record_description";

            switch (configName)
            {
                case @"MySql5.5.19":
                    getDateTimeSql = mysqlGetDateTimeSql;
                    expectedDateTimes = new DateTime?[,]
                        {
                            {
                                DateTime.ParseExact(@"9999-12-31", "yyyy-MM-dd", CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"9999-12-31 23:59:59", "yyyy-MM-dd HH:mm:ss",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2038-01-18 19:14:07", "yyyy-MM-dd HH:mm:ss",
                                                    CultureInfo.InvariantCulture)
                            },
                            {
                                DateTime.ParseExact(@"1000-01-01", "yyyy-MM-dd", CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"1000-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"1970-01-01 00:00:01", "yyyy-MM-dd HH:mm:ss",
                                                    CultureInfo.InvariantCulture)
                            },
                            {
                                DateTime.ParseExact(@"2013-01-01", "yyyy-MM-dd", CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2013-01-01 01:01:01", "yyyy-MM-dd HH:mm:ss",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2013-01-01 01:01:01", "yyyy-MM-dd HH:mm:ss",
                                                    CultureInfo.InvariantCulture)
                            },
                            {null, null, null}
                        };
                    break;
                case @"MySql5.6.11":
                    getDateTimeSql = mysqlGetDateTimeSql;
                    expectedDateTimes = new DateTime?[,]
                        {
                            {
                                DateTime.ParseExact(@"9999-12-31", "yyyy-MM-dd", CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"9999-12-31 23:59:59.999999", "yyyy-MM-dd HH:mm:ss.ffffff",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2038-01-18 19:14:07.999999", "yyyy-MM-dd HH:mm:ss.ffffff",
                                                    CultureInfo.InvariantCulture)
                            },
                            {
                                DateTime.ParseExact(@"1000-01-01", "yyyy-MM-dd", CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"1000-01-01 00:00:00.000000", "yyyy-MM-dd HH:mm:ss.ffffff",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"1970-01-01 00:00:01.000000", "yyyy-MM-dd HH:mm:ss.ffffff",
                                                    CultureInfo.InvariantCulture)
                            },
                            {
                                DateTime.ParseExact(@"2013-01-01", "yyyy-MM-dd", CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2013-01-01 01:01:01.123456", "yyyy-MM-dd HH:mm:ss.ffffff",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2013-01-01 01:01:01.123456", "yyyy-MM-dd HH:mm:ss.ffffff",
                                                    CultureInfo.InvariantCulture)
                            },
                            {null, null, null}
                        };
                    break;
                case @"Oracle":
                    getDateTimeSql =
                        @"select xdate,xtimestamp,xtimestamptz,xtimestampltz from datetimes_tab where record_description not like '%min' order by record_description";
                    expectedDateTimes = new DateTime?[,]
                        {
                            {
                                DateTime.ParseExact(@"9999-12-31 23:59:59", "yyyy-MM-dd HH:mm:ss",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"9999-12-31 23:59:59.9999999", "yyyy-MM-dd HH:mm:ss.fffffff",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"9999-12-31 23:59:59.9999999 +14:00",
                                                    "yyyy-MM-dd HH:mm:ss.fffffff zzz", CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"9999-12-30 00:59:59.9999999",
                                                    "yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture)
                            },
                            {
                                DateTime.ParseExact(@"2013-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2013-01-01 01:01:01.1234567", "yyyy-MM-dd HH:mm:ss.fffffff",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2013-01-01 01:01:01.1234567 -08:00",
                                                    "yyyy-MM-dd HH:mm:ss.fffffff zzz", CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2013-01-01 02:01:01.1234567",
                                                    "yyyy-MM-dd HH:mm:ss.fffffff", CultureInfo.InvariantCulture)
                            },
                            {null, null, null, null}
                        };
                    break;
                case @"SqlServ2008":
                    getDateTimeSql =
                        @"select xdate, xsmalldatetime, xdatetime, xdatetime2, xdatetimeoffset from datetimes_tab order by record_description";
                    expectedDateTimes = new DateTime?[,]
                        {
                            {
                                DateTime.ParseExact(@"9999-12-31", "yyyy-MM-dd", CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2079-06-06 23:59:00", "yyyy-MM-dd HH:mm:ss",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"9999-12-31 23:59:59.997", "yyyy-MM-dd HH:mm:ss.fff",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"9999-12-31 23:59:59.9999999", "yyyy-MM-dd HH:mm:ss.fffffff",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"9999-12-31 23:59:59.9999999 +14:00",
                                                    "yyyy-MM-dd HH:mm:ss.fffffff zzz", CultureInfo.InvariantCulture)
                            },
                            {
                                DateTime.ParseExact(@"0001-01-01", "yyyy-MM-dd", CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"1900-01-01 00:00:00", "yyyy-MM-dd HH:mm:ss",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"1753-01-01 00:00:00.000", "yyyy-MM-dd HH:mm:ss.fff",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"0001-01-01 00:00:00.0000000", "yyyy-MM-dd HH:mm:ss.fffffff",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"0001-01-01 00:00:00.0000000 -14:00",
                                                    "yyyy-MM-dd HH:mm:ss.fffffff zzz", CultureInfo.InvariantCulture)
                            },
                            {
                                DateTime.ParseExact(@"2013-01-01", "yyyy-MM-dd", CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2013-01-01 01:01:00", "yyyy-MM-dd HH:mm:ss",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2013-01-01 01:01:01.123", "yyyy-MM-dd HH:mm:ss.fff",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2013-01-01 01:01:01.1234567", "yyyy-MM-dd HH:mm:ss.fffffff",
                                                    CultureInfo.InvariantCulture),
                                DateTime.ParseExact(@"2013-01-01 01:01:01.1234567 -08:00",
                                                    "yyyy-MM-dd HH:mm:ss.fffffff zzz", CultureInfo.InvariantCulture)
                            },
                            {null, null, null, null, null}
                        };
                    break;
            }

            WithinAReaderLoopRun(providerName, connectionString, getDateTimeSql, expectedDateTimes.GetLength(0),
                                 (reader, rows, fieldCount) =>
                                 {
                                     DateTime? result = null;
                                     reader.GetDateTime(((Reader)reader).GetColumnName(fieldCount), ref result);
                                     Assert.AreEqual(expectedDateTimes[rows, fieldCount], result);
                                 });
        }

        [TestCaseSource("ConnectionNameAndData")]
        public void GetDateTimeThrowsArgumentOutOfRangeExceptionForDateTimeOutsideDotNetDateTimeRange(string configName,
                                                                                                      string
                                                                                                          providerName,
                                                                                                      string
                                                                                                          connectionString)
        {
            var defaultValue = DateTime.Now;
            var getDateTimeSql = "";
            var expectedDateTimeRows = 1;

            const string mysqlGetDateTimeStringSql =
                @"select xdate, xdatetime, xtimestamp from datetimes_tab where record_description like '%invalid'";

            switch (configName)
            {
                case @"MySql5.5.19":
                case @"MySql5.6.11":
                    getDateTimeSql = mysqlGetDateTimeStringSql;
                    break;
                case @"Oracle":
                    getDateTimeSql =
                        @"select xdate,xtimestamp,xtimestamptz,xtimestampltz from datetimes_tab where record_description like '%min'";
                    break;
                case @"SqlServ2008":
                    getDateTimeSql =
                        @"select null from datetimes_tab where 1=2";
                    expectedDateTimeRows = 0;
                    break;
            }

            WithinAReaderLoopRun(providerName, connectionString, getDateTimeSql, expectedDateTimeRows,
                                 (reader, rows, fieldCount) =>
                                 Assert.Throws(Is.TypeOf<ArgumentOutOfRangeException>(), () =>
                                     {
                                         var result = defaultValue;
                                         reader.GetDateTime(((Reader)reader).GetColumnName(fieldCount), ref result);
                                     }));

            WithinAReaderLoopRun(providerName, connectionString, getDateTimeSql, expectedDateTimeRows,
                                 (reader, rows, fieldCount) =>
                                 Assert.Throws(Is.TypeOf<ArgumentOutOfRangeException>(), () =>
                                 {
                                     DateTime? result = null;
                                     reader.GetDateTime(((Reader)reader).GetColumnName(fieldCount), ref result);
                                 }));
        }


        [TestCaseSource("ConnectionNameAndData")]
        public void GetDateTimeStringGetsCorrectDateTimeAsString(string configName, string providerName,
                                                                 string connectionString)
        {
            var defaultValue = DateTime.Now.ToString(CultureInfo.InvariantCulture);
            var getDateTimeStringSql = "";
            var expectedDateTimesStrings = new string[,] {{}};

            const string mysqlGetDateTimeStringSql =
                @"select xdate, xdatetime, xtimestamp from datetimes_tab order by record_description";

            switch (configName)
            {
                case @"MySql5.5.19":
                    getDateTimeStringSql = mysqlGetDateTimeStringSql;
                    expectedDateTimesStrings = new[,]
                        {
                            {
                                @"9999-12-31 00:00:00.000000000", @"9999-12-31 23:59:59.000000000",
                                @"2038-01-18 19:14:07.000000000"
                            },
                            {
                                @"1000-01-01 00:00:00.000000000", @"1000-01-01 00:00:00.000000000",
                                @"1970-01-01 00:00:01.000000000"
                            },
                            {
                                @"2013-01-01 00:00:00.000000000", @"2013-01-01 01:01:01.000000000",
                                @"2013-01-01 01:01:01.000000000"
                            },
                            {
                                @"0000-00-00 00:00:00.000000000", @"0000-00-00 00:00:00.000000000",
                                @"0000-00-00 00:00:00.000000000"
                            },
                            {
                                defaultValue, defaultValue, defaultValue
                            }
                        };

                    break;
                case @"MySql5.6.11":
                    getDateTimeStringSql = mysqlGetDateTimeStringSql;
                    expectedDateTimesStrings = new[,]
                        {
                            {
                                @"9999-12-31 00:00:00.000000000", @"9999-12-31 23:59:59.999999000",
                                @"2038-01-18 19:14:07.999999000"
                            },
                            {
                                @"1000-01-01 00:00:00.000000000", @"1000-01-01 00:00:00.000000000",
                                @"1970-01-01 00:00:01.000000000"
                            },
                            {
                                @"2013-01-01 00:00:00.000000000", @"2013-01-01 01:01:01.123456000",
                                @"2013-01-01 01:01:01.123456000"
                            },
                            {
                                @"0000-00-00 00:00:00.000000000", @"0000-00-00 00:00:00.000000000",
                                @"0000-00-00 00:00:00.000000000"
                            },
                            {
                                defaultValue, defaultValue, defaultValue
                            }
                        };
                    break;
                case @"Oracle":
                    getDateTimeStringSql =
                        @"select xdate,xtimestamp,xtimestamptz,xtimestampltz from datetimes_tab order by record_description";
                    expectedDateTimesStrings = new[,]
                        {
                            {
                                @"9999-12-31 23:59:59.000000000",
                                @"9999-12-31 23:59:59.999999999",
                                @"9999-12-31 23:59:59.999999999 +14:00",
                                @"9999-12-30 00:59:59.999999999"
                            },
                            {
                                @"-4712-12-31 23:59:59.000000000",
                                @"-4712-12-31 23:59:59.999999999",
                                @"-4712-12-31 23:59:59.999999999 -12:00",
                                @"-4712-12-30 00:59:59.999999999"
                            },
                            {
                                @"2013-01-01 00:00:00.000000000",
                                @"2013-01-01 01:01:01.123456789",
                                @"2013-01-01 01:01:01.123456789 -08:00",
                                @"2013-01-01 02:01:01.123456789"
                            },
                            {
                                defaultValue, defaultValue, defaultValue, defaultValue
                            }
                        };
                    break;
                case @"SqlServ2008":
                    getDateTimeStringSql =
                        @"select xdate, xsmalldatetime, xdatetime, xdatetime2, xdatetimeoffset from datetimes_tab order by record_description";
                    expectedDateTimesStrings = new[,]
                        {
                            {
                                @"9999-12-31 00:00:00.000000000",
                                @"2079-06-06 23:59:00.000000000",
                                @"9999-12-31 23:59:59.997000000",
                                @"9999-12-31 23:59:59.999999900",
                                @"9999-12-31 23:59:59.999999900 +14:00"
                            },
                            {
                                @"0001-01-01 00:00:00.000000000",
                                @"1900-01-01 00:00:00.000000000",
                                @"1753-01-01 00:00:00.000000000",
                                @"0001-01-01 00:00:00.000000000",
                                @"0001-01-01 00:00:00.000000000 -14:00"
                            },
                            {
                                @"2013-01-01 00:00:00.000000000",
                                @"2013-01-01 01:01:00.000000000",
                                @"2013-01-01 01:01:01.123000000",
                                @"2013-01-01 01:01:01.123456700",
                                @"2013-01-01 01:01:01.123456700 -08:00"
                            },
                            {defaultValue, defaultValue, defaultValue, defaultValue, defaultValue}
                        };
                    break;
            }

            WithinAReaderLoopRun(providerName, connectionString, getDateTimeStringSql,
                                 expectedDateTimesStrings.GetLength(0),
                                 (reader, rows, fieldCount) =>
                                     {
                                         var result = defaultValue;
                                         reader.GetDateTimeString(((Reader)reader).GetColumnName(fieldCount), ref result);
                                         Assert.AreEqual(expectedDateTimesStrings[rows, fieldCount], result);
                                     });
        }

        //todo: more Gets_datatype_Values tests

        [TestCaseSource("ConnectionData")]
        public void IsDisposed(string providerName, string connectionString)
        {
            Assert.Throws(Is.TypeOf<ObjectDisposedException>(), () =>
                                                                WithADbAccessRun(providerName, connectionString,
                                                                                 db =>
                                                                                     {
                                                                                         using (
                                                                                             var cmd =
                                                                                                 db.CreateCommand())
                                                                                         {
                                                                                             cmd.CommandText =
                                                                                                 @"select * from ten_row_tab";
                                                                                             var reader =
                                                                                                 cmd.ExecuteReader();
                                                                                             reader.Dispose();
                                                                                             reader.AdvanceRow();
                                                                                         }
                                                                                     }));
        }
    }
}