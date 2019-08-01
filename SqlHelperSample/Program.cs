using Microsoft.Data.SqlClient.Helpers;
using Microsoft.Extensions.Configuration;
using System;
using System.IO;

namespace SqlHelperSample
{
    class Program
    {
        private static IConfigurationRoot _config;

        static void Main(string[] args)
        {
            var builder = new ConfigurationBuilder()
           .SetBasePath(Directory.GetCurrentDirectory())
           .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

            //load the config and store it in memory
            _config = builder.Build();

            //set the loader connection string loader to get the connection string from the configuration
            DataConnectionStringManager.ConnectionStringLoader = (key) =>
            {
                return _config.GetConnectionString(key);
            };

            DataConnectionStringManager.SetConnectionString("Test", "Data Source=MSSQL; Initial Catalog = master;  Integrated Security=True; MultipleActiveResultSets=True");

            DataConnectionStringManager.AddOverride("Test", "Data Source=MSSQL; Initial Catalog = master;  Integrated Security=True; MultipleActiveResultSets=True");

            using (var aDc = new DataConnection(DataConnectionStringManager.GetConnectionString("Default")))
            {
                var dtresults = aDc.QueryAsync("Select * from Customers");
            }

        }
    }
}
