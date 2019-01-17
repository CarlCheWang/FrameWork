using System;
using System.Data;
using System.Data.SqlTypes;
using System.Globalization;
using MySql.Data.MySqlClient;
using MySql.Data.Types;

namespace PlatformSupport.DatabaseAccess
{
    internal sealed class MySqlReaderWrapper : Reader
    {
        private DateTime ConvertToDateTime(MySqlDateTime dbDateTime)
        {
            var fsString = dbDateTime.Millisecond.ToString("d").PadRight(7, '0');
            var dtString = dbDateTime.GetDateTime().ToString("yyyy-MM-dd HH:mm:ss");

            return DateTime.ParseExact(dtString + "." + fsString, "yyyy-MM-dd HH:mm:ss.fffffff",
                                       CultureInfo.InvariantCulture);
        }

        private string ConvertDateTimeToString(MySqlDateTime dbDateTime)
        {
            var fractionalSeconds = dbDateTime.Millisecond.ToString("d").PadRight(9, '0');
            const string timezone = "";
            return ConvertToString(dbDateTime.Year, dbDateTime.Month, dbDateTime.Day, dbDateTime.Hour, dbDateTime.Minute,
                                   dbDateTime.Second, fractionalSeconds, timezone).TrimEnd(' ');
        }

        internal MySqlReaderWrapper(UsageEnforcer enforcer, IDataReader reader)
            : base(enforcer, reader)
        {
        }

        public override void GetDecimalString(string columnName, ref string value)
        {
            Enforcer.VerifyValidReader(this);           
            var columnIndex = DbReader.GetOrdinal(columnName);                     

            var dbDecimal = ((MySqlDataReader)DbReader).GetMySqlDecimal(columnIndex);
            if (!dbDecimal.IsNull) value = dbDecimal.ToString();
        }

        public override void GetDateTime(string columnName, ref DateTime value)
        {
            try
            {
                int columnIndex;
                if (CanGetValue(columnName, out columnIndex))
                {
                    var dbDateTime = ((MySqlDataReader) DbReader).GetMySqlDateTime(columnIndex);
                    value = ConvertToDateTime(dbDateTime);
                }
            }
            catch (MySqlConversionException mysqlException)
            {
                throw new ArgumentOutOfRangeException("value cannot be converted to DateTime", mysqlException);
            }
        }

        public override void GetDateTime(string columnName, ref DateTime? value)
        {
            try
            {
                int columnIndex;
                if (CanGetValue(columnName, out columnIndex))
                {
                    var dbDateTime = ((MySqlDataReader)DbReader).GetMySqlDateTime(columnIndex);
                    value = ConvertToDateTime(dbDateTime);
                }
            }
            catch (MySqlConversionException mysqlException)
            {
                throw new ArgumentOutOfRangeException("value cannot be converted to DateTime", mysqlException);
            }
        }

        public override void GetDateTimeString(string columnName, ref string value)
        {
            Enforcer.VerifyValidReader(this);
            var columnIndex = DbReader.GetOrdinal(columnName);

            try
            {
                var dbDateTime = ((MySqlDataReader)DbReader).GetMySqlDateTime(columnIndex);
                value = ConvertDateTimeToString(dbDateTime);
            }
            catch (SqlNullValueException)
            {
            }
        }

    }
}
