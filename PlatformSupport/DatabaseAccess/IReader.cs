using System;
using System.Data;

namespace PlatformSupport.DatabaseAccess
{
    public interface IReader : IDisposable
    {
        bool AdvanceRow();

        int FieldCount { get; }

        /// <summary>
        /// format for returning database datetime as string: yyyy-MM-dd HH:mm:ss.fffffffff zzz
        /// </summary>
        string DbDateTimeAsStringFormat { get; }

        DataTable GetSchemaTable();

        /// <summary>
        /// for databases which don't support the boolean data type, method returns true when a numeric type value equals 1
        /// </summary>
        void GetBoolean(string columnName, ref bool value);
        /// <summary>
        /// for databases which don't support the boolean data type, method returns true when a numeric type value equals 1
        /// </summary>
        void GetBoolean(string columnName, ref bool? value);

        void GetByte(string columnName, ref byte value);
        void GetByte(string columnName, ref byte? value);

        void GetInt16(string columnName, ref short value);
        void GetInt16(string columnName, ref short? value);
        
        void GetInt(string columnName, ref int value);
        void GetInt(string columnName, ref int? value);

        void GetInt64(string columnName, ref long value);
        void GetInt64(string columnName, ref long? value);

        /// <summary>
        /// throws OverflowException when database numeric value exceeds the range of .NET decimal 
        /// <para>user should code for possible exception, and use GetDecimalString to return string value</para>
        /// </summary>
        void GetDecimal(string columnName, ref decimal value);
        /// <summary>
        /// throws OverflowException when database numeric value exceeds the range of .NET decimal 
        /// <para>user should code for possible exception, and use GetDecimalString to return string value</para>
        /// </summary>
        void GetDecimal(string columnName, ref decimal? value);

        /// <summary>
        /// gets database numeric value as a string
        /// <para>intended for use when the database numeric value exceeds the range of .NET decimal</para>
        /// </summary>
        void GetDecimalString(string columnName, ref string value);

        void GetFloat(string columnName, ref float value);
        void GetFloat(string columnName, ref float? value);

        void GetDouble(string columnName, ref double value);
        void GetDouble(string columnName, ref double? value);

        void GetString(string columnName, ref string value);

        /// <summary>
        /// gets database datetime at highest .NET datetime resolution (100 nanoseconds, aka tick); resolution beyond 1 tick will be lost
        /// <para>to preserve value with higher resolution than 1 tick, use GetDateTimeString to return string value</para>
        /// <para>throws ArgumentOutOfRangeException exception when database datetime value exceeds the range of .NET datetime</para>
        /// <para>user should code for possible exception, and use GetDateTimeString to return string value</para>
        /// </summary>
        void GetDateTime(string columnName, ref DateTime value);
        /// <summary>
        /// gets database datetime at highest .NET datetime resolution (100 nanoseconds, aka tick); resolution beyond 1 tick will be lost
        /// <para>to preserve value with higher resolution than 1 tick, use GetDateTimeString to return string value</para>
        /// <para>throws ArgumentOutOfRangeException exception when database datetime value exceeds the range of .NET datetime</para>
        /// <para>user should code for possible exception, and use GetDateTimeString to return string value</para>
        /// </summary>
        void GetDateTime(string columnName, ref DateTime? value);

        /// <summary>
        /// gets datebase datetime value as a string in DateTimeStringFormat format
        /// </summary>
        void GetDateTimeString(string columnName, ref string value);

    }
}
