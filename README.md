# Microsoft.Data.SqlClient.Helpers
Data Access Classes and extensions for System.Data.Common and Microsoft.Data.SqlClient

## Features
- Simplfied API for running Queries and Executing Scripts
- Helper methods for insterting and updating data without script
- Connection string management helpers

## Usage

### DataConnectionStringManager
`DataConnectionStringManager` provides a centralised location for storing and managing connections strings across projects in a solution.

This solves the issue of the connection string loading mechanisms only being available on the host application.

#### Loading connection strings

The `DataConnectionStringManager.ConnectionStringLoader` property allows you to set a function to handle loading of a connection string when requested by shared code, which cannot access the app.config or web.config files.

This code shows a .NET Core console application example

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
    }

This code shows a .NET Framework application example

    DataConnectionStringManager.ConnectionStringLoader = (key) =>
    {
        return ConfigurationManager.ConnectionStrings[key].ConnectionString;
    });

You can also manually add a ConnectionString by calling `DataConnectionStringManager.SetConnectionString`

    DataConnectionStringManager.SetConnectionString("Test", "Data Source=MSSQL; Initial Catalog = SalesDatabase;  Integrated Security=True; MultipleActiveResultSets=True");

Lastly, you can add an override, in debug mode for example for a connection string using the `DataConnectionStringManager.AddOverride` method

    #if DEBUG
        DataConnectionStringManager.AddOverride("Test", "Data Source=MSSQL; Initial Catalog = SalesDatabaseDev;  Integrated Security=True; MultipleActiveResultSets=True");
    #endif


#### Get connection strings

To access a connection string from `DataConnectionStringManager` you can use the static method `DataConnectionStringManager.GetConnectionString` method.

    using (var aDc = new DataConnection(DataConnectionStringManager.GetConnectionString("Default")))
    {

    }

This will try a return the Connection string in the following order

- `DataConnectionStringManager.ConnectionStringLoader` (if set and returns a value)
- Check for an override set via `DataConnectionStringManager.AddOverride`
- Check for a entry in `DataConnectionStringManager.Instance.ConnectionStrings`

### DataConnection
`DataConnection` is a SqlClient based class that provides simplified access to a MS Sql database connection with an easy to use API

Creating a new instance is fairly simple

    using (var aDc = new DataConnection(DataConnectionStringManager.GetConnectionString("Default")))
    {

    }


#### SQL Methods
There are a number of standard SQL execution methods that allow you to run and execute SQL script on the SQL Server.

Note: `DataConnection` will handle opening and connecting to the server itself, you don't need to explicitly do it

This is a simple query that returns a DataTable with the results of the query

    using (var aDc = new DataConnection(DataConnectionStringManager.GetConnectionString("Default")))
    {
        var dtresults = await aDc.QueryAsync("Select * from Customers");
    }

This is the same query with a list of parameters and a where statement


    using (var aDc = new DataConnection(DataConnectionStringManager.GetConnectionString("Default")))
    {
        var pars = new List<DbParameter>()
        {
            new SqlParameter("@CustomerId",12345)
        };

        var sSql = "Select * from Customers";
        sSql += " WHERE CustomerId = @CustomerId";


        var dtresults = await aDc.QueryAsync(sSql, pars);
    }

You can also query a stored procedure and return the results

    using (var aDc = new DataConnection(DataConnectionStringManager.GetConnectionString("Default")))
    {
        var pars2 = new List<DbParameter>()
        {
            new SqlParameter("@CustomerId",12345)
        };

        var dtresults2 = await aDc.QueryAsync("GetCustomerInvoices", pars, CommandType.StoredProcedure);
    }

Executing SQL statements is also as simple:

    using (var aDc = new DataConnection(DataConnectionStringManager.GetConnectionString("Default")))
    {
        var pars3 = new List<DbParameter>()
        {
            new SqlParameter("@CustomerId",12345),
            new SqlParameter("@OrderNo",12345),
            new SqlParameter("@OrderStatus", "Confirmed"),
        };

        await aDc.ExecuteAsync("UpdateOrderStatus", pars3, CommandType.StoredProcedure);
    }

#### Non-SQL Methods
`DataConnection` also provides some non-sql methods for inserting and updating data without writing SQL at all.

`DataConnection.UpdateManyAsync` allows you to provide a dictionary of columns and values to update, along with a dictionary for filtering the records to be updated, which can be used to update the specific table without writing SQL script

    using (var aDc = new DataConnection(DataConnectionStringManager.GetConnectionString("Default")))
    {
        var dict = new Dictionary<string, object>();
        dict.Add("LastUpdated", DateTime.Now);
        dict.Add("UpdatedById", allocatedById);

        aTran = DataConnection.BeginTransaction();

        var wheredict = new Dictionary<string, object>();
        wheredict.Add("DatabaseName", databaseName);
        wheredict.Add("Tran_Number", transactionId);

        await DataConnection.UpdateManyAsync("JobExpenses", wheredict, dict, transaction: aTran);

        aTran.Commit();
    }

## Extensions

There are a number of extensions that help we passing values through to the database, as parameters.  

- `DateTime.AsValueOrDbNull`
  - Will return DBNull.Value if the DateTime? object has no value
- `DataRow.WhenValid`
  - Simplified convert a column value to a real value
- `NameValueCollection.ValueAsInt`
  - Returns the value as int if its not null and can be converted to an int
- `string.AsValueOrDbNull`
  - Return a DBNull.Value if the string is empty, null or whitespace or the value otherwise
- `string.AsValueOrDefault`
  - Returns the default value if the string is empty, null or whitespace or the value otherwise
