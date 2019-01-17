using System.Data;

namespace PlatformSupport.DatabaseAccess
{
    public enum SupportedIsolation
    {
        Default = IsolationLevel.ReadCommitted,
        Serializable = IsolationLevel.Serializable
    }
}
