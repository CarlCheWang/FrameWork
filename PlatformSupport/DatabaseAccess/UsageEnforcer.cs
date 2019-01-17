using System;
using System.Collections.Generic;
using System.Linq;

namespace PlatformSupport.DatabaseAccess
{
    internal class UsageEnforcer
    {
        private EnforcedDbAccess _enforcedDbAccess;
        private readonly List<EnforcedTransaction> _enforcedTransactions;        
        private readonly List<EnforcedCommand> _enforcedCommands;
        private readonly List<EnforcedReader> _enforcedReaders;

        internal UsageEnforcer(IDbAccess db)
        {
            _enforcedDbAccess = new EnforcedDbAccess(db);
            _enforcedCommands = new List<EnforcedCommand>();
            _enforcedTransactions = new List<EnforcedTransaction>();
            _enforcedReaders = new List<EnforcedReader>();
        }

        internal void DisposeDbAccess()
        {
            _enforcedDbAccess = null;
        }

        internal void CreatedTransaction(ITransaction transaction)
        {
            var enforcedTran = new EnforcedTransaction(transaction, false);
            _enforcedTransactions.Add(enforcedTran);
        }

        internal void EndTransaction(ITransaction transaction)
        {
            var pendingTrans = _enforcedTransactions.Where(t => t.Transaction == transaction && (!t.Ended)).ToList();
            if (pendingTrans.Count > 1)
            {
                var ex = new InvalidOperationException("More than one pending transaction found.");
                _enforcedDbAccess.InnerException = ex;
                throw ex;
            }
            pendingTrans[0].Ended = true;
        }

        internal void DisposeTransaction(ITransaction transaction)
        {
            var removed = _enforcedTransactions.RemoveAll(t => t.Transaction == transaction);
            if (removed != 1)
            {
                var ex = (removed == 0) ? new InvalidOperationException("Disposing untracked transaction.") : 
                    new InvalidOperationException("Disposing multiple transactions.");
                _enforcedDbAccess.InnerException = ex;
                throw ex;
            }
        }

        internal void CreatedCommand(ICommand command)
        {
            var enforcedCmd = new EnforcedCommand(command);
            _enforcedCommands.Add(enforcedCmd);
        }

        internal void DisposeCommand(ICommand command)
        {
            var removed = _enforcedCommands.RemoveAll(c => c.Command == command);
            if (removed != 1)
            {
                var ex = (removed == 0) ? new InvalidOperationException("Disposing untracked command.") :
                    new InvalidOperationException("Disposing multiple commands.");
                _enforcedDbAccess.InnerException = ex;
                throw ex;
            }
        }

        internal void CreatedReader(ICommand command, IReader reader)
        {
            var enforcedRdr = new EnforcedReader(command, reader);
            _enforcedReaders.Add(enforcedRdr);
        }

        internal void DisposeReader(IReader reader)
        {
            EnforcedCommand enforcedCommand;
            try
            {
                enforcedCommand = _enforcedCommands.Single(c => c.Command == (_enforcedReaders.Single(r => r.Reader == reader).Command));
            }
            catch (Exception exception)
            {
                var ex = new InvalidOperationException(String.Format("Error identifying parent command; {0}", exception.Message));
                _enforcedDbAccess.InnerException = ex;
                throw ex;
            }
            
            var removed = _enforcedReaders.RemoveAll(r => r.Reader == reader);
            if (removed != 1)
            {
                var ex = (removed == 0) ? new InvalidOperationException("Disposing untracked reader.") :
                    new InvalidOperationException("Disposing multiple readers.");
                enforcedCommand.InnerException = ex;
                throw ex;
            }
        }

        internal void VerifyValidDbAccess()
        {
            if (_enforcedDbAccess == null)
            {
                throw new ObjectDisposedException("DbAccess has been disposed.");
            }
        }

        internal void VerifyDbAccessIsDisposable()
        {
            if (_enforcedCommands.Count > 0 || _enforcedTransactions.Count > 0)
            {
                throw new InvalidOperationException(
                    "Cannot dispose DbAccess when there are undisposed transactions and/or commands.",
                    _enforcedDbAccess.InnerException);
            }
        }

        internal void VerifyCanBeginTransaction()
        {
            VerifyValidDbAccess();

            if (_enforcedCommands.Count > 0 || _enforcedTransactions.Any(t => (!t.Ended)))
            {
                var ex = (_enforcedCommands.Count > 0) ? new InvalidOperationException("Cannot begin transaction when there are undisposed commands.") :
                        new InvalidOperationException("Cannot begin transaction when there is pending transaction.");
                _enforcedDbAccess.InnerException = ex;
                throw ex;
            }
        }

        internal void VerifyTransactionHasNoUndisposedCommands(ITransaction transaction)
        {
            if (_enforcedCommands.Count > 0)
            {
                var ex = new InvalidOperationException("Cannot dispose transaction when there are undisposed commands.");
                _enforcedDbAccess.InnerException = ex;
                throw ex;
            }            
        }

        internal void VerifyTransactionWasExplicitlyEnded(ITransaction transaction)
        {
            if (_enforcedTransactions.Any(t => t.Transaction == transaction && (!t.Ended)))
            {
                var ex = new InvalidOperationException("Cannot dispose transaction which has not been committed or rolled back.");
                _enforcedDbAccess.InnerException = ex;
                throw ex;
            }
        }

        internal void VerifyTransactionIsDisposable(ITransaction transaction)
        {
            VerifyTransactionHasNoUndisposedCommands(transaction);
            VerifyTransactionWasExplicitlyEnded(transaction);
        }

        internal void VerifyValidTransaction(ITransaction transaction)
        {
            if (_enforcedTransactions.All(t => t.Transaction != transaction))
            {
                throw new ObjectDisposedException("Transaction has been disposed.");
            }
        }

        internal void VerifyCanEndTransaction(ITransaction transaction)
        {
            VerifyValidTransaction(transaction);

            EnforcedTransaction enforcedTransaction;
            try
            {
                enforcedTransaction = _enforcedTransactions.Single(t => t.Transaction == transaction);
            }
            catch (Exception exception)
            {
                var ex = new InvalidOperationException(String.Format("Error identifying current transaction; {0}", exception.Message));
                _enforcedDbAccess.InnerException = ex;
                throw ex;
            }

            if (enforcedTransaction.Ended) throw new InvalidOperationException("Transaction has already ended; it is no longer usable.");

        }

        internal void VerifyValidCommand(ICommand command)
        {
            if (_enforcedCommands.All(c => c.Command != command))
            {
                throw new ObjectDisposedException("Command has been disposed.");
            }
        }

        internal void VerifyCommandIsDisposable(ICommand command)
        {
            if (_enforcedReaders.Count(r => r.Command == command) > 0)
            {
                var ex = new InvalidOperationException("Cannot dispose command when there are undisposed readers.");
                _enforcedDbAccess.InnerException = ex;
                throw ex;
            }
        }

        internal void VerifyValidReader(IReader reader)
        {
            if (_enforcedReaders.All(r => r.Reader != reader))
            {
                throw new ObjectDisposedException("Reader has been disposed.");
            }
        }

    }
}
