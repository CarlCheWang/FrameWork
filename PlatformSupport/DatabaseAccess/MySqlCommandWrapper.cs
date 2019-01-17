using System.Data;

namespace PlatformSupport.DatabaseAccess
{
    internal sealed class MySqlCommandWrapper : Command
    {
        protected override IReader ExecuteReader(CommandBehavior? commandBehavior)
        {
            Enforcer.VerifyValidCommand(this);

            var dbReader = commandBehavior != null
                               ? DbCommand.ExecuteReader((CommandBehavior)commandBehavior)
                               : DbCommand.ExecuteReader();
            var reader = new MySqlReaderWrapper(Enforcer, dbReader);
            Enforcer.CreatedReader(this, reader);

            return reader;
        }

        internal MySqlCommandWrapper(UsageEnforcer enforcer, IDbCommand command)
            : base(enforcer, command)
        {
        }

        protected override void Dispose(bool disposing)
        {            
            using (DbCommand)
            {
            }
            DbCommand = null;

            Enforcer.VerifyCommandIsDisposable(this);
            Enforcer.DisposeCommand(this);
        }

    }
}
