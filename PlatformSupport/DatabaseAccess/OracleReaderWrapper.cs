using System;
using System.Data;
using Oracle.ManagedDataAccess.Client;

namespace PlatformSupport.DatabaseAccess
{
    internal sealed class OracleReaderWrapper : Reader
    {
        private DateTime ConvertToDateTime(int columnIndex)
        {
            if (DbReader.GetDataTypeName(columnIndex) == OracleDbType.Date.ToString() ||
                DbReader.GetDataTypeName(columnIndex) == OracleDbType.TimeStamp.ToString() ||
                DbReader.GetDataTypeName(columnIndex) == OracleDbType.TimeStampLTZ.ToString()
                )
            {
                return DbReader.GetDateTime(columnIndex);
            }

            if (DbReader.GetDataTypeName(columnIndex) == OracleDbType.TimeStampTZ.ToString())
            {
                var dbDateTime = ((OracleDataReader)DbReader).GetOracleTimeStampTZ(columnIndex);
                var dateTimeString = new DateTime().AddTicks(dbDateTime.Value.Ticks).ToString("o");
                return DateTime.Parse(dateTimeString + " " + dbDateTime.TimeZone);
            }

            throw new InvalidCastException(String.Format("column {0} is not a valid datetime type!",
                                                          DbReader.GetName(columnIndex)));
        }

        private string ConvertDateTimeToString(int columnIndex)
        {
            var fractionalSeconds = "000000000";
            var timezone = "";
            if (DbReader.GetDataTypeName(columnIndex) == OracleDbType.Date.ToString())
            {
                var dbDateTime = ((OracleDataReader) DbReader).GetOracleDate(columnIndex);
                return ConvertToString(dbDateTime.Year, dbDateTime.Month, dbDateTime.Day, dbDateTime.Hour,
                                       dbDateTime.Minute, dbDateTime.Second, fractionalSeconds, timezone);
            }
            if (DbReader.GetDataTypeName(columnIndex) == OracleDbType.TimeStamp.ToString())
            {
                var dbDateTime = ((OracleDataReader) DbReader).GetOracleTimeStamp(columnIndex);
                fractionalSeconds = dbDateTime.Nanosecond.ToString("000000000");
                return ConvertToString(dbDateTime.Year, dbDateTime.Month, dbDateTime.Day, dbDateTime.Hour,
                                       dbDateTime.Minute, dbDateTime.Second, fractionalSeconds, timezone);
            }
            if (DbReader.GetDataTypeName(columnIndex) == OracleDbType.TimeStampTZ.ToString())
            {
                var dbDateTime = ((OracleDataReader) DbReader).GetOracleTimeStampTZ(columnIndex);
                fractionalSeconds = dbDateTime.Nanosecond.ToString("000000000");
                timezone = dbDateTime.TimeZone;
                return ConvertToString(dbDateTime.Year, dbDateTime.Month, dbDateTime.Day, dbDateTime.Hour,
                                       dbDateTime.Minute, dbDateTime.Second, fractionalSeconds, timezone);
            }
            if (DbReader.GetDataTypeName(columnIndex) == OracleDbType.TimeStampLTZ.ToString())
            {
                var dbDateTime = ((OracleDataReader) DbReader).GetOracleTimeStampLTZ(columnIndex);
                fractionalSeconds = dbDateTime.Nanosecond.ToString("000000000");
                return ConvertToString(dbDateTime.Year, dbDateTime.Month, dbDateTime.Day, dbDateTime.Hour,
                                       dbDateTime.Minute, dbDateTime.Second, fractionalSeconds, timezone);
            }

            throw new InvalidCastException(String.Format("column {0} is not a valid datetime type!",
                                                         DbReader.GetName(columnIndex)));
        }

        internal OracleReaderWrapper(UsageEnforcer enforcer, IDataReader reader)
            : base(enforcer, reader)
        {
        }

        public override void GetBoolean(string columnName, ref bool value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex))
            {
                var v = DbReader.GetDecimal(columnIndex);
                value = (v == 1);
            }
        }

        public override void GetBoolean(string columnName, ref bool? value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex))
            {
                var v = DbReader.GetDecimal(columnIndex);
                value = (v == 1);
            }
        }

        public override void GetDecimalString(string columnName, ref string value)
        {
            Enforcer.VerifyValidReader(this);
            var columnIndex = DbReader.GetOrdinal(columnName);

            var dbDecimal = ((OracleDataReader)DbReader).GetOracleDecimal(columnIndex);
            if (!dbDecimal.IsNull) value = dbDecimal.ToString();
        }

        public override void GetFloat(string columnName, ref float value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = Convert.ToSingle(DbReader.GetValue(columnIndex));
        }

        public override void GetFloat(string columnName, ref float? value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = Convert.ToSingle(DbReader.GetValue(columnIndex));
        }

        public override void GetDateTime(string columnName, ref DateTime value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = ConvertToDateTime(columnIndex);
        }

        public override void GetDateTime(string columnName, ref DateTime? value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = ConvertToDateTime(columnIndex);
        }

        public override void GetDateTimeString(string columnName, ref string value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = ConvertDateTimeToString(columnIndex);
        }
    }
}
