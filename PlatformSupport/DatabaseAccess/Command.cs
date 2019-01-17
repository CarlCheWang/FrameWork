using System;
using System.Data;

namespace PlatformSupport.DatabaseAccess
{
    internal abstract class Command : ICommand
    {
        protected UsageEnforcer Enforcer;
        protected IDbCommand DbCommand;

        internal Command(UsageEnforcer enforcer, IDbCommand command)
        {
            Enforcer = enforcer;
            DbCommand = command;
        }

        public SupportedCommandType CommandType
        {
            set
            {
                Enforcer.VerifyValidCommand(this);
                DbCommand.CommandType = (CommandType)value;
            }
            get
            {
                Enforcer.VerifyValidCommand(this);
                return (SupportedCommandType)DbCommand.CommandType;
            }
        }

        public string CommandText
        {
            set
            {
                Enforcer.VerifyValidCommand(this);
                DbCommand.CommandText = value;
            }
            get
            {
                Enforcer.VerifyValidCommand(this);
                return DbCommand.CommandText;
            }
        }

        public int CommandTimeout
        {
            set
            {
                Enforcer.VerifyValidCommand(this);
                DbCommand.CommandTimeout = value;
            }
            get
            {
                Enforcer.VerifyValidCommand(this);
                return DbCommand.CommandTimeout;
            }
        }

        public int ExecuteNonQuery()
        {
            Enforcer.VerifyValidCommand(this);
            return DbCommand.ExecuteNonQuery();
        }

        public IReader ExecuteReader()
        {
            return ExecuteReader(null);
        }

        public IReader ExecuteReader(CommandBehavior commandBehavior)
        {
            return ExecuteReader((CommandBehavior?)commandBehavior);
        }

        protected abstract IReader ExecuteReader(CommandBehavior? commandBehavior);

        public object ExecuteScalar()
        {
            Enforcer.VerifyValidCommand(this);

            var result = DbCommand.ExecuteScalar();
            return (result ?? DBNull.Value);
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected abstract void Dispose(bool disposing);

    }
}
