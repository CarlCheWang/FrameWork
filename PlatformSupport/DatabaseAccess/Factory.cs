using System;

namespace PlatformSupport.DatabaseAccess
{
    public class Factory
    {
        private readonly ProviderFactoriesAdapter _adapter;

        public Factory()
        {
            _adapter = new ProviderFactoriesAdapter();
        }

        public Factory(ProviderFactoriesAdapter adapter)
        {
            _adapter = adapter;
        }

        public IDbAccess CreateDbAccess(string providerName, string connectionString)
        {
            var providerFactory = _adapter.GetFactory(providerName);
            
            IDbAccess dbAccess;
            switch (providerName)
            {
                case "MySql.Data.MySqlClient":
                    dbAccess = new MySqlDbAccess(providerFactory, connectionString);
                    break;
                case "Oracle.ManagedDataAccess.Client":
                    dbAccess = new OracleDbAccess(providerFactory, connectionString);
                    break;
                //case "System.Data.SqlClient":
                //    dbAccess = new SqlServerDbAccess(providerFactory, connectionString);
                //    break;
                //case "System.Data.SqlServerCe.3.5":
                //    dbAccess = new SqlCeDbAccess(providerFactory, connectionString);
                //    break;
                default:
                    throw new ArgumentException();
            }
            return dbAccess;
        }
    }
}
