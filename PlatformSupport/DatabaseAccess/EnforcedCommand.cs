using System;

namespace PlatformSupport.DatabaseAccess
{
    internal class EnforcedCommand
    {
        internal ICommand Command { get; set; }

        internal Exception InnerException { get; set; }

        internal EnforcedCommand(ICommand command)
        {
            Command = command;
        }
    }
}
