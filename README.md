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

