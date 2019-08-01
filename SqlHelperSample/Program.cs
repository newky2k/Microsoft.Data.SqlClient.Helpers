using Microsoft.Data.SqlClient;
using Microsoft.Data.SqlClient.Helpers;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.IO;
using System.Threading.Tasks;

namespace SqlHelperSample
{
    class Program
    {
        private static IConfigurationRoot _config;

        static async Task Main(string[] args)
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
                var pars = new List<DbParameter>()
                {
                   new SqlParameter("@CustomerId",12345)
                };

                var sSql = "Select * from Customers";
                sSql += " WHERE CustomerId = @CustomerId";


                var dtresults = await aDc.QueryAsync(sSql, pars);

                var pars2 = new List<DbParameter>()
                {
                   new SqlParameter("@CustomerId",12345)
                };

                var dtresults2 = await aDc.QueryAsync("GetCustomerInvoices", pars, CommandType.StoredProcedure);

                var pars3 = new List<DbParameter>()
                {
                   new SqlParameter("@CustomerId",12345),
                   new SqlParameter("@OrderNo",12345),
                   new SqlParameter("@OrderStatus", "Confirmed"),
                };

                await aDc.ExecuteAsync("UpdateOrderStatus", pars3, CommandType.StoredProcedure);
            }

        }
    }
}
