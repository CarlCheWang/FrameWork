using System;

namespace PlatformSupport.DatabaseAccess
{
    internal class EnforcedReader
    {
        internal ICommand Command { get; set; }

        internal IReader Reader { get; set; }

        internal Exception InnerException { get; set; }

        internal EnforcedReader(ICommand command, IReader reader)
        {
            Command = command;
            Reader = reader;
        }
    }
}
