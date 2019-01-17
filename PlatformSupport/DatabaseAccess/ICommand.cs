using System;
using System.Data;

namespace PlatformSupport.DatabaseAccess
{
    public interface ICommand : IDisposable
    {
        SupportedCommandType CommandType { set; get; }

        string CommandText { set; get; }

        int CommandTimeout { set; get; }

        int ExecuteNonQuery();

        IReader ExecuteReader();

        IReader ExecuteReader(CommandBehavior commandBehavior);

        /// <summary>
        /// Returns the first column of the first row of the query result set. If no row is found, or if the column value is a database null, returns a DBNull.Value.
        /// </summary>
        object ExecuteScalar();
    }
}
