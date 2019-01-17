using System.Data;
using Oracle.ManagedDataAccess.Client;

namespace PlatformSupport.DatabaseAccess
{
    internal sealed class OracleCommandWrapper : Command
    {
        protected override IReader ExecuteReader(CommandBehavior? commandBehavior)
        {
            Enforcer.VerifyValidCommand(this);

            var dbReader = commandBehavior != null
                               ? DbCommand.ExecuteReader((CommandBehavior)commandBehavior)
                               : DbCommand.ExecuteReader();
            var reader = new OracleReaderWrapper(Enforcer, dbReader);
            Enforcer.CreatedReader(this, reader);

            return reader;
        }

        internal OracleCommandWrapper(UsageEnforcer enforcer, IDbCommand command) : base(enforcer, command)
        {
            ((OracleCommand)command).BindByName = true;
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
