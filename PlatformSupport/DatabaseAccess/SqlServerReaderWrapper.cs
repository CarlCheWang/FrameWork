using System;
using System.Data;
using System.Data.SqlClient;

namespace PlatformSupport.DatabaseAccess
{
    internal sealed class SqlServerReaderWrapper : Reader
    {
        private string GetFieldTypeName(int columnIndex)
        {
            var fieldType = DbReader.GetFieldType(columnIndex);
            if (fieldType != null) return fieldType.Name;
            return "";
        }

        static DateTime ConvertFromDateTimeOffset(DateTimeOffset dateTimeOffset)
        {
            var dateTimeString = dateTimeOffset.DateTime.ToString("o");
            var offset = dateTimeOffset.Offset;
            var timezoneString = GetTimezone(offset);
            return DateTime.Parse(dateTimeString + " " + timezoneString);
        }

        private DateTime ConvertToDateTime(int columnIndex)
        {
            var fieldTypeName = GetFieldTypeName(columnIndex);
            if (GetFieldTypeName(columnIndex) == SqlDbType.DateTime.ToString())
            {
                return DbReader.GetDateTime(columnIndex);
            }

            if (fieldTypeName == SqlDbType.DateTimeOffset.ToString())
            {
                var dbDateTime = ((SqlDataReader)DbReader).GetDateTimeOffset(columnIndex);
                return ConvertFromDateTimeOffset(dbDateTime);
            }

            throw new InvalidCastException(String.Format("column {0} is not a valid datetime type!",
                                                          DbReader.GetName(columnIndex)));
        }

        private static void GetFractionalSeconds(TimeSpan timeOfDay, ref string fractionalSeconds)
        {
            var timeOfDayPieces = timeOfDay.ToString().Split('.');
            if (timeOfDayPieces.Length == 2)
            {
                fractionalSeconds = timeOfDayPieces[1].Trim().PadRight(9, '0');
            }
        }

        private static string GetTimezone(TimeSpan offset)
        {
            var tzHour = offset.Hours.ToString("D2");
            var tzMin = offset.Minutes.ToString("D2");
            return (offset.Hours > -1 ? "+" : "") + tzHour + ":" + tzMin;
        }

        private string ConvertDateTimeToString(int columnIndex)
        {
            var fractionalSeconds = "000000000";
            var timezone = "";

            var fieldTypeName = GetFieldTypeName(columnIndex);
            if (GetFieldTypeName(columnIndex) == SqlDbType.DateTime.ToString())
            {
                var dbDateTime = DbReader.GetDateTime(columnIndex);
                GetFractionalSeconds(dbDateTime.TimeOfDay, ref fractionalSeconds);

                return ConvertToString(dbDateTime.Year, dbDateTime.Month, dbDateTime.Day, dbDateTime.Hour,
                                       dbDateTime.Minute, dbDateTime.Second, fractionalSeconds, timezone);
            }

            if (fieldTypeName == SqlDbType.DateTimeOffset.ToString())
            {
                var dbDateTime = ((SqlDataReader)DbReader).GetDateTimeOffset(columnIndex);
                GetFractionalSeconds(dbDateTime.TimeOfDay, ref fractionalSeconds);
                timezone = GetTimezone(dbDateTime.Offset);
                return ConvertToString(dbDateTime.Year, dbDateTime.Month, dbDateTime.Day, dbDateTime.Hour,
                                       dbDateTime.Minute, dbDateTime.Second, fractionalSeconds, timezone);
            }

            throw new InvalidCastException(String.Format("column {0} is not a valid datetime type!",
                                                         DbReader.GetName(columnIndex)));
        }


        internal SqlServerReaderWrapper(UsageEnforcer enforcer, IDataReader reader)
            : base(enforcer, reader)
        {
        }

        public override void GetInt16(string columnName, ref short value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = Convert.ToInt16(DbReader.GetValue(columnIndex));
        }

        public override void GetInt16(string columnName, ref short? value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = Convert.ToInt16(DbReader.GetValue(columnIndex));
        }

        public override void GetInt(string columnName, ref int value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = Convert.ToInt32(DbReader.GetValue(columnIndex));
        }

        public override void GetInt(string columnName, ref int? value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = Convert.ToInt32(DbReader.GetValue(columnIndex));
        }

        public override void GetInt64(string columnName, ref long value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = Convert.ToInt64(DbReader.GetValue(columnIndex));
        }

        public override void GetInt64(string columnName, ref long? value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = Convert.ToInt64(DbReader.GetValue(columnIndex));
        }

        public override void GetDecimal(string columnName, ref decimal value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = Convert.ToDecimal(DbReader.GetValue(columnIndex));
        }

        public override void GetDecimal(string columnName, ref decimal? value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = Convert.ToDecimal(DbReader.GetValue(columnIndex));
        }

        public override void GetDecimalString(string columnName, ref string value)
        {
            Enforcer.VerifyValidReader(this);
            var columnIndex = DbReader.GetOrdinal(columnName);

            var dbDecimal = ((SqlDataReader)DbReader).GetSqlDecimal(columnIndex);
            if (!dbDecimal.IsNull) value = dbDecimal.ToString();
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
