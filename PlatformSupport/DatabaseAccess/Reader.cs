using System;
using System.Data;
using System.Data.Common;

namespace PlatformSupport.DatabaseAccess
{
    internal abstract class Reader : IReader
    {
        protected readonly UsageEnforcer Enforcer;
        protected DbDataReader DbReader;

        internal Reader(UsageEnforcer enforcer, IDataReader reader)
        {
            Enforcer = enforcer;
            DbReader = (DbDataReader)reader;
        }

        internal string GetColumnName(int columnIndex)
        {
            Enforcer.VerifyValidReader(this);
            return DbReader.GetName(columnIndex);
        }

        protected bool CanGetValue(string columnName, out int columnIndex)
        {
            Enforcer.VerifyValidReader(this);
            columnIndex = DbReader.GetOrdinal(columnName);
            if (DbReader.IsDBNull(columnIndex)) return false;
            return true;
        }

        protected bool CanGetValue(int columnIndex)
        {
            Enforcer.VerifyValidReader(this);
            if (DbReader.IsDBNull(columnIndex)) return false;
            return true;
        }

        protected string ConvertToString(int year, int month, int day, int hour, int minute, int second, string fracSecond,
                                         string timezone)
        {
            return
                String.Format(DbDateTimeAsStringFormat, year, month, day, hour, minute, second, fracSecond, timezone).TrimEnd(' ');
        }


        public bool AdvanceRow()
        {
            Enforcer.VerifyValidReader(this);
            return DbReader.Read();
        }

        public int FieldCount
        {
            get
            {
                Enforcer.VerifyValidReader(this);
                return DbReader.FieldCount;
            }
        }

        public string DbDateTimeAsStringFormat
        {
            get
            {
                Enforcer.VerifyValidReader(this);
                return @"{0:0000}-{1:00}-{2:00} {3:00}:{4:00}:{5:00}.{6,9} {7}";
            }
        }

        public DataTable GetSchemaTable()
        {
            return DbReader.GetSchemaTable();
        }

        public virtual void GetBoolean(string columnName, ref bool value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = DbReader.GetBoolean(columnIndex);
        }

        public virtual void GetBoolean(string columnName, ref bool? value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = DbReader.GetBoolean(columnIndex);
        }

        public void GetByte(string columnName, ref byte value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = DbReader.GetByte(columnIndex);
        }

        public void GetByte(string columnName, ref byte? value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = DbReader.GetByte(columnIndex);
        }

        public virtual void GetInt16(string columnName, ref short value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = DbReader.GetInt16(columnIndex);
        }

        public virtual void GetInt16(string columnName, ref short? value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = DbReader.GetInt16(columnIndex);
        }

        public virtual void GetInt(string columnName, ref int value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = DbReader.GetInt32(columnIndex);
        }

        public virtual void GetInt(string columnName, ref int? value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = DbReader.GetInt32(columnIndex);
        }

        public virtual void GetInt64(string columnName, ref long value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = DbReader.GetInt64(columnIndex);
        }

        public virtual void GetInt64(string columnName, ref long? value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = DbReader.GetInt64(columnIndex);
        }

        public virtual void GetDecimal(string columnName, ref decimal value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = DbReader.GetDecimal(columnIndex);
        }

        public virtual void GetDecimal(string columnName, ref decimal? value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = DbReader.GetDecimal(columnIndex);
        }

        public abstract void GetDecimalString(string columnName, ref string value);

        public virtual void GetFloat(string columnName, ref float value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = DbReader.GetFloat(columnIndex);
        }

        public virtual void GetFloat(string columnName, ref float? value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = DbReader.GetFloat(columnIndex);
        }

        public void GetDouble(string columnName, ref double value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = Convert.ToDouble(DbReader.GetValue(columnIndex));
        }

        public void GetDouble(string columnName, ref double? value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = Convert.ToDouble(DbReader.GetValue(columnIndex));
        }

        public void GetString(string columnName, ref string value)
        {
            int columnIndex;
            if (CanGetValue(columnName, out columnIndex)) value = DbReader.GetString(columnIndex);
        }

        public abstract void GetDateTime(string columnName, ref DateTime value);
        public abstract void GetDateTime(string columnName, ref DateTime? value);

        public abstract void GetDateTimeString(string columnName, ref string value);

        public void Dispose()
        {
            Dispose(true);
        }
        
        protected void Dispose(bool disposing)
        {
            using (DbReader)
            {
            }
            DbReader = null;

            Enforcer.DisposeReader(this);
        }

    }
}
