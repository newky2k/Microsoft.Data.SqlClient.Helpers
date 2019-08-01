using System;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.Odbc;
using System.Linq;
using System.Threading.Tasks;

namespace System.Data.Common
{
    /// <summary>
    /// Extensions for DbConnection and sub-classses
    /// </summary>
    public static class DbConnectionExtension
    {
        #region Non-Async
        /// <summary>
        /// Checks if the table or view exists
        /// </summary>
        /// <typeparam name="T">Sub-class of DbConnection</typeparam>
        /// <param name="connection">The connection object</param>
        /// <param name="tableViewName">Name of the table or view.</param>
        /// <returns><c>true</c> if exists, <c>false</c> otherwise.</returns>
        public static bool DoesTableViewExist<T>(this T connection, string tableViewName) where T : DbConnection
        {
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }

            var dtItems = connection.GetSchema("Tables",
                           new string[] { null, null, tableViewName });

            return dtItems.Rows.Count > 0;
        }

        /// <summary>
        /// Executes the database script
        /// </summary>
        /// <typeparam name="T">Sub-class of DbConnection</typeparam>
        /// <param name="connection">The connection. object</param>
        /// <param name="databaseScript">The database script to execute</param>
        /// <param name="pars">The parameters</param>
        /// <param name="commandType">Type of the command</param>
        /// <param name="timeout">The timeout</param>
        /// <param name="transaction">Optional transaction</param>
        public static void Execute<T>(this T connection, string databaseScript, List<DbParameter> pars = null, CommandType commandType = CommandType.Text, int timeout = 30, DbTransaction transaction = null) where T : DbConnection
        {
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }

            var newCmq = connection.CreateCommand();

            if (transaction != null)
            {
                newCmq.Transaction = transaction;
            }

            newCmq.CommandType = commandType;
            newCmq.CommandText = databaseScript;
            newCmq.CommandTimeout = timeout;

            if (pars != null)
                newCmq.Parameters.AddRange(pars.ToArray());

            newCmq.ExecuteNonQuery();
        }

        /// <summary>
        /// Executes the query and returns the first column of the first row in the result set returned by the query. Additional columns or rows are ignored.
        /// </summary>
        /// <typeparam name="T">DBConnection sub-class</typeparam>
        /// <param name="connection">The connection object</param>
        /// <param name="databaseScript">The database script to execute</param>
        /// <param name="pars">The parameters</param>
        /// <param name="commandType">Type of the command</param>
        /// <param name="timeout">The timeout</param>
        /// <param name="transaction">Optional transaction</param>
        /// <returns>System.Object.</returns>
        public static object ExecuteScalar<T>(this T connection, string databaseScript, List<DbParameter> pars = null, CommandType commandType = CommandType.Text, int timeout = 30, DbTransaction transaction = null) where T : DbConnection
        {
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }

            var newCmq = connection.CreateCommand();

            if (transaction != null)
            {
                newCmq.Transaction = transaction;
            }

            newCmq.CommandType = commandType;
            newCmq.CommandText = databaseScript;
            newCmq.CommandTimeout = timeout;

            if (pars != null)
                newCmq.Parameters.AddRange(pars.ToArray());

            return newCmq.ExecuteScalar();

        }


        /// <summary>
        /// Runs the specified database script and returns a DataTable with the results
        /// </summary>
        /// <typeparam name="T">DBConnection sub-class</typeparam>
        /// <param name="connection">The connection object</param>
        /// <param name="databaseScript">The database script to execute</param>
        /// <param name="pars">The parameters</param>
        /// <param name="commandType">Type of the command</param>
        /// <param name="timeout">The timeout</param>
        /// <returns>DataTable.</returns>
        /// <exception cref="AggregateException"></exception>
        public static DataTable Query<T>(this T connection, string databaseScript, List<DbParameter> pars = null, CommandType commandType = CommandType.Text, int timeout = 30) where T : DbConnection
        {

            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }

            var newCmq = connection.CreateCommand();
            newCmq.CommandType = commandType;
            newCmq.CommandText = databaseScript;
            newCmq.CommandTimeout = timeout;

            if (pars != null)
                newCmq.Parameters.AddRange(pars.ToArray());

            try
            {

                var dt = new DataTable();
                var dA = connection.CreateDataAdapter();
                dA.SelectCommand = newCmq;
                dA.Fill(dt);

                return dt;
            }
            catch (Exception ex)
            {
                var message = String.Format("Error occured running the query: {0}", databaseScript);

                throw new AggregateException(message, ex);
            }

        }

        /// <summary>
        /// Inserts multiple data items into the specified tables and columns without needing database script
        /// </summary>
        /// <typeparam name="T">DBConnection sub-class</typeparam>
        /// <param name="connection">The connection object</param>
        /// <param name="tableViewName">Name of the table or view</param>
        /// <param name="data">The data dictionary, string = ColumnName, object = value</param>
        /// <param name="timeout">The timeout</param>
        /// <param name="transaction">Optional transaction</param>
        /// <exception cref="Exception">You must provide a non-null, non-empty dictionary to InsertMany</exception>
        public static void InsertMany<T>(this T connection, string tableViewName, Dictionary<string, object> data, int timeout = 30, DbTransaction transaction = null) where T : DbConnection
        {
            if (data == null && data.Count == 0)
                throw new Exception("You must provide a non-null, non-empty dictionary to InsertManyAsync");

            var comm = connection.CreateCommand();
            comm.CommandType = CommandType.Text;
            comm.CommandTimeout = timeout;

            //get the column names
            var colNames = new List<string>()
            {

            };

            colNames.AddRange(data.Keys.ToList());


            //build the parameters
            var parsLoop = 1;
            var parsList = new List<String>();

            foreach (var aKey in data.Keys)
            {
                var theValue = data[aKey];

                var par1 = comm.CreateParameter();
                par1.ParameterName = $"@Param{parsLoop}";


                par1.Value = (theValue == null) ? DBNull.Value : theValue;

                comm.Parameters.Add(par1);

                parsList.Add(par1.ParameterName);

                parsLoop++;
            }


            //build the insert sql
            var databaseScript = $"INSERT INTO {tableViewName} ";

            var coldatabaseScript = "(";

            coldatabaseScript += String.Join(",", colNames);

            coldatabaseScript += ") values (";

            coldatabaseScript += string.Join(",", parsList);

            coldatabaseScript += ")";

            databaseScript += coldatabaseScript;

            comm.CommandText = databaseScript;

            if (transaction != null)
                comm.Transaction = transaction;

            comm.ExecuteNonQuery();

        }

        /// <summary>
        /// Gets the DataTable schema from table.
        /// </summary>
        /// <typeparam name="T">DBConnection sub-class</typeparam>
        /// <param name="connection">The connection object</param>
        /// <param name="tableViewName">Name of the table or view</param>
        /// <param name="transaction">Optional transaction</param>
        /// <returns>DataTable</returns>
        public static DataTable GetDataTableSchemaFromTable<T>(this T connection, string tableViewName, DbTransaction transaction = null) where T : DbConnection
        {
            DataTable dtResult = new DataTable();

            using (var command = connection.CreateCommand())
            {
                command.CommandText = String.Format("SELECT TOP 1 * FROM {0}", tableViewName);
                command.CommandType = CommandType.Text;

                if (transaction != null)
                {
                    command.Transaction = transaction;
                }

                var reader = command.ExecuteReader(CommandBehavior.SchemaOnly);


                dtResult.Load(reader);

            }

            return dtResult;
        }
        #endregion

        #region Async

        /// <summary>
        /// Executes the database script as an asynchronous operation.
        /// </summary>
        /// <typeparam name="T">DBConnection sub-class</typeparam>
        /// <param name="connection">The connection object</param>
        /// <param name="databaseScript">The database script to execute</param>
        /// <param name="pars">The parameters</param>
        /// <param name="commandType">Type of the command</param>
        /// <param name="timeout">The timeout</param>
        /// <param name="transaction">Optional transaction</param>
        public static async Task ExecuteAsync<T>(this T connection, string databaseScript, List<DbParameter> pars = null, CommandType commandType = CommandType.Text, int timeout = 30, DbTransaction transaction = null) where T : DbConnection
        {
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }

            var newCmq = connection.CreateCommand();

            if (transaction != null)
            {
                newCmq.Transaction = transaction;
            }

            newCmq.CommandText = databaseScript;
            newCmq.CommandType = commandType;
            newCmq.CommandTimeout = timeout;

            if (pars != null)
                newCmq.Parameters.AddRange(pars.ToArray());

            await newCmq.ExecuteNonQueryAsync();


        }


        /// <summary>
        /// Executes the query as an asynchronous operation and returns the first column of the first row in the result set returned by the query. Additional columns or rows are ignored. 
        /// </summary>
        /// <typeparam name="T">DBConnection sub-class</typeparam>
        /// <param name="connection">The connection object</param>
        /// <param name="databaseScript">The database script to execute</param>
        /// <param name="pars">The parameters</param>
        /// <param name="commandType">Type of the command</param>
        /// <param name="timeout">The timeout</param>
        /// <param name="transaction">Optional transaction</param>
        /// <returns>Task&lt;System.Object&gt;.</returns>
        public static async Task<object> ExecuteScalarAsync<T>(this T connection, string databaseScript, List<DbParameter> pars = null, CommandType commandType = CommandType.Text, int timeout = 30, DbTransaction transaction = null) where T : DbConnection
        {
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }

            var newCmq = connection.CreateCommand();

            if (transaction != null)
            {
                newCmq.Transaction = transaction;
            }

            newCmq.CommandText = databaseScript;
            newCmq.CommandType = commandType;
            newCmq.CommandTimeout = timeout;

            if (pars != null)
                newCmq.Parameters.AddRange(pars.ToArray());

            return await newCmq.ExecuteScalarAsync();
        }


        /// <summary>
        /// Executes an insert as an asynchronous operation and returns the first column of the first row as an int
        /// </summary>
        /// <typeparam name="T">DBConnection sub-class</typeparam>
        /// <param name="connection">The connection object</param>
        /// <param name="databaseScript">The database script to execute</param>
        /// <param name="pars">The parameters</param>
        /// <param name="commandType">Type of the command</param>
        /// <param name="timeout">The timeout</param>
        /// <param name="transaction">Optional transaction</param>
        /// <returns>Task&lt;System.Int32&gt;.</returns>
        public static async Task<int> ExecuteInsertAsync<T>(this T connection, string databaseScript, List<DbParameter> pars = null, CommandType commandType = CommandType.Text, int timeout = 30, DbTransaction transaction = null) where T : DbConnection
        {
            var res = await connection.ExecuteScalarAsync(databaseScript, pars, commandType, timeout, transaction);

            return Convert.ToInt32(res);

        }

        /// <summary>
        /// Runs the specified database script as an asynchronous operation and returns a DataTable with the results
        /// </summary>
        /// <typeparam name="T">DBConnection sub-class</typeparam>
        /// <param name="connection">The connection object</param>
        /// <param name="databaseScript">The database script to execute</param>
        /// <param name="pars">The parameters</param>
        /// <param name="commandType">Type of the command</param>
        /// <param name="timeout">The timeout</param>
        /// <returns>Task&lt;DataTable&gt;.</returns>
        public static async Task<DataTable> QueryAsync<T>(this T connection, string databaseScript, List<DbParameter> pars = null, CommandType commandType = CommandType.Text, int timeout = 30) where T : DbConnection
        {
            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync();
            }

            var newCmq = connection.CreateCommand();
            newCmq.CommandType = commandType;
            newCmq.CommandText = databaseScript;
            newCmq.CommandTimeout = timeout;

            if (pars != null)
                newCmq.Parameters.AddRange(pars.ToArray());

            var result = await Task.Factory.StartNew<DataTable>(() =>
            {
                try
                {
                    var dt = new DataTable();

                    var dA = connection.CreateDataAdapter();
                    dA.SelectCommand = newCmq;
                    dA.Fill(dt);

                    return dt;
                }
                catch (Exception ex)
                {
                    var message = String.Format("Error occured running the query: {0}", databaseScript);

                    throw new Exception(message, ex);
                }

            });


            return result;

        }

        /// <summary>
        /// Checks to see if the record already exists as an asynchronous operation
        /// </summary>
        /// <typeparam name="T">DBConnection sub-class</typeparam>
        /// <param name="connection">The connection object</param>
        /// <param name="tableViewName">Name of the table or view</param>
        /// <param name="columnName">Name of the column to compare</param>
        /// <param name="value">The value to compare</param>
        /// <param name="timeout">The timeout</param>
        /// <returns>Task&lt;System.Boolean&gt;.</returns>
        public static async Task<bool> ExistsAsync<T>(this T connection, string tableViewName, string columnName, object value, int timeout = 30) where T : DbConnection
        {

            var databaseScript = $"SELECT {columnName} from {tableViewName}";
            databaseScript += $" WHERE {columnName} = @Param1";

            var comm = connection.CreateCommand();
            comm.CommandType = CommandType.Text;
            comm.CommandText = databaseScript;
            comm.CommandTimeout = timeout;

            var par1 = comm.CreateParameter();
            par1.ParameterName = "@Param1";
            par1.Value = value;
            comm.Parameters.Add(par1);

            var result = await Task.Factory.StartNew<DataTable>(() =>
            {
                try
                {
                    var dt = new DataTable();

                    var dA = connection.CreateDataAdapter();
                    dA.SelectCommand = comm;
                    dA.Fill(dt);

                    return dt;
                }
                catch (Exception ex)
                {
                    var message = String.Format("Error occured running the query: {0}", databaseScript);

                    throw new Exception(message, ex);
                }

            });


            return (result.Rows.Count > 0);

        }

        /// <summary>
        /// Checks to see if the record already exists as an asynchronous operation
        /// </summary>
        /// <typeparam name="T">DBConnection sub-class</typeparam>
        /// <param name="connection">The connection object</param>
        /// <param name="tableViewName">Name of the table or view</param>
        /// <param name="columnName">Name of the column</param>
        /// <param name="whereParams">The where parameter dictionary, string = ColumnName, object = value</param>
        /// <param name="timeout">The timeout</param>
        /// <returns>Task&lt;System.Boolean&gt;.</returns>
        public static async Task<bool> ExistsAsync<T>(this T connection, string tableViewName, string columnName, Dictionary<string, object> whereParams, int timeout = 30) where T : DbConnection
        {
            var databaseScript = $"SELECT {columnName} from {tableViewName}";

            var comm = connection.CreateCommand();
            comm.CommandType = CommandType.Text;
            comm.CommandTimeout = timeout;

            if (whereParams != null && whereParams.Count > 0)
                databaseScript += comm.BuildWhere(whereParams);

            comm.CommandText = databaseScript;


            var result = await Task.Factory.StartNew<DataTable>(() =>
            {
                try
                {
                    var dt = new DataTable();

                    var dA = connection.CreateDataAdapter();
                    dA.SelectCommand = comm;
                    dA.Fill(dt);

                    return dt;
                }
                catch (Exception ex)
                {
                    var message = String.Format("Error occured running the query: {0}", databaseScript);

                    throw new Exception(message, ex);
                }

            });


            return (result.Rows.Count > 0);

        }

        /// <summary>
        /// Inserts one data item as an asynchronous operation, without needing database script 
        /// </summary>
        /// <typeparam name="T">DBConnection sub-class</typeparam>
        /// <param name="connection">The connection object</param>
        /// <param name="tableViewName">Name of the table or view</param>
        /// <param name="idColumn">The identifier column name</param>
        /// <param name="idValue">The identifier value name</param>
        /// <param name="valueColumn">The value column name</param>
        /// <param name="value">The value to insert</param>
        /// <param name="timeout">The timeout</param>
        /// <param name="transaction">Optional transaction</param>
        public static async Task InsertOneAsync<T>(this T connection, string tableViewName, string idColumn, object idValue, string valueColumn, object value, int timeout = 30, DbTransaction transaction = null) where T : DbConnection
        {
            var databaseScript = $"INSERT INTO {tableViewName} ({idColumn}, {valueColumn}) values (@Param1, @Param2)";

            var comm = connection.CreateCommand();
            comm.CommandType = CommandType.Text;
            comm.CommandText = databaseScript;
            comm.CommandTimeout = timeout;

            var par1 = comm.CreateParameter();
            par1.ParameterName = "@Param1";
            par1.Value = idValue;
            comm.Parameters.Add(par1);

            var par2 = comm.CreateParameter();
            par2.ParameterName = "@Param2";
            par2.Value = value;
            comm.Parameters.Add(par2);

            if (transaction != null)
                comm.Transaction = transaction;

            await comm.ExecuteNonQueryAsync();

        }

        /// <summary>
        /// Updates one data item as an asynchronous operation, without needing database script 
        /// </summary>
        /// <typeparam name="T">DBConnection sub-class</typeparam>
        /// <param name="connection">The connection object</param>
        /// <param name="tableViewName">Name of the table or view</param>
        /// <param name="idColumn">The identifier column</param>
        /// <param name="idValue">The identifier value name</param>
        /// <param name="valueColumn">The value column name</param>
        /// <param name="value">The value</param>
        /// <param name="timeout">The timeout</param>
        /// <param name="transaction">Optional transaction</param>
        public static async Task UpdateOneAsync<T>(this T connection, string tableViewName, string idColumn, object idValue, string valueColumn, object value, int timeout = 30, DbTransaction transaction = null) where T : DbConnection
        {
            var databaseScript = $"UPDATE {tableViewName}";
            databaseScript += $" SET {valueColumn} = @Param2";
            databaseScript += $" WHERE {idColumn} = @Param1";

            var comm = connection.CreateCommand();
            comm.CommandType = CommandType.Text;
            comm.CommandText = databaseScript;
            comm.CommandTimeout = timeout;

            var par1 = comm.CreateParameter();
            par1.ParameterName = "@Param1";
            par1.Value = idValue;
            comm.Parameters.Add(par1);

            var par2 = comm.CreateParameter();
            par2.ParameterName = "@Param2";
            par2.Value = value;
            comm.Parameters.Add(par2);

            if (transaction != null)
                comm.Transaction = transaction;

            await comm.ExecuteNonQueryAsync();

        }

        /// <summary>
        /// Inserts multiple data items into the specified tables and columns as an asynchronous operation, without needing database script
        /// </summary>
        /// <typeparam name="T">DBConnection sub-class</typeparam>
        /// <param name="connection">The connection object</param>
        /// <param name="tableViewName">Name of the table or view</param>
        /// <param name="idColumn">The identifier column</param>
        /// <param name="idValue">The identifier value</param>
        /// <param name="data">The data dictionary, string = ColumnName, object = value</param>
        /// <param name="timeout">The timeout</param>
        /// <param name="transaction">Optional transaction</param>
        /// <exception cref="Exception">You must provide a non-null, non-empty dictionary to InsertManyAsync</exception>
        public static async Task InsertManyAsync<T>(this T connection, string tableViewName, string idColumn, object idValue, Dictionary<string, object> data, int timeout = 30, DbTransaction transaction = null) where T : DbConnection
        {
            if (data == null && data.Count == 0)
                throw new Exception("You must provide a non-null, non-empty dictionary to InsertManyAsync");

            var comm = connection.CreateCommand();
            comm.CommandType = CommandType.Text;
            comm.CommandTimeout = timeout;

            //get the column names
            var colNames = new List<string>()
            {
                idColumn,
            };

            colNames.AddRange(data.Keys.ToList());


            //build the parameters
            var parsLoop = 1;
            var parsList = new List<String>();

            var idPar = comm.CreateParameter();
            idPar.ParameterName = $"@Param{parsLoop}";
            idPar.Value = idValue;
            comm.Parameters.Add(idPar);
            parsList.Add(idPar.ParameterName);

            parsLoop++;

            foreach (var aKey in data.Keys)
            {
                var theValue = data[aKey];

                var par1 = comm.CreateParameter();
                par1.ParameterName = $"@Param{parsLoop}";
                par1.Value = theValue;
                comm.Parameters.Add(par1);

                parsList.Add(par1.ParameterName);

                parsLoop++;
            }


            //build the insert sql
            var databaseScript = $"INSERT INTO {tableViewName} ";

            var coldatabaseScript = "(";

            coldatabaseScript += String.Join(",", colNames);

            coldatabaseScript += ") values (";

            coldatabaseScript += string.Join(",", parsList);

            coldatabaseScript += ")";

            databaseScript += coldatabaseScript;

            comm.CommandText = databaseScript;

            if (transaction != null)
                comm.Transaction = transaction;

            await comm.ExecuteNonQueryAsync();

        }

        /// <summary>
        /// Inserts multiple data items into the specified tables and columns as an asynchronous operation, without needing datbase script
        /// </summary>
        /// <typeparam name="T">DBConnection sub-class</typeparam>
        /// <param name="connection">The connection object</param>
        /// <param name="tableViewName">Name of the table or view</param>
        /// <param name="data">The data dictionary, string = ColumnName, object = value</param>
        /// <param name="timeout">The timeout</param>
        /// <param name="transaction">Optional transaction</param>
        /// <exception cref="Exception">You must provide a non-null, non-empty dictionary to InsertManyAsync</exception>
        public static async Task InsertManyAsync<T>(this T connection, string tableViewName, Dictionary<string, object> data, int timeout = 30, DbTransaction transaction = null) where T : DbConnection
        {
            if (data == null && data.Count == 0)
                throw new Exception("You must provide a non-null, non-empty dictionary to InsertManyAsync");

            var comm = connection.CreateCommand();
            comm.CommandType = CommandType.Text;
            comm.CommandTimeout = timeout;

            //get the column names
            var colNames = new List<string>()
            {

            };

            colNames.AddRange(data.Keys.ToList());


            //build the parameters
            var parsLoop = 1;
            var parsList = new List<String>();

            foreach (var aKey in data.Keys)
            {
                var theValue = data[aKey];

                var par1 = comm.CreateParameter();
                par1.ParameterName = $"@Param{parsLoop}";


                par1.Value = (theValue == null) ? DBNull.Value : theValue;

                comm.Parameters.Add(par1);

                parsList.Add(par1.ParameterName);

                parsLoop++;
            }


            //build the insert sql
            var databaseScript = $"INSERT INTO {tableViewName} ";

            var coldatabaseScript = "(";

            coldatabaseScript += String.Join(",", colNames);

            coldatabaseScript += ") values (";

            coldatabaseScript += string.Join(",", parsList);

            coldatabaseScript += ")";

            databaseScript += coldatabaseScript;

            comm.CommandText = databaseScript;

            if (transaction != null)
                comm.Transaction = transaction;

            await comm.ExecuteNonQueryAsync();

        }

        /// <summary>
        /// Update multiple data items in the specified tables and columns as an asynchronous operation, without needing datbase script
        /// </summary>
        /// <typeparam name="T">DBConnection sub-class</typeparam>
        /// <param name="connection">The connection object</param>
        /// <param name="tableViewName">Name of the table or view</param>
        /// <param name="idColumn">The identifier column</param>
        /// <param name="idValue">The identifier value</param>
        /// <param name="data">The data dictionary, string = ColumnName, object = value</param>
        /// <param name="timeout">The timeout</param>
        /// <param name="transaction">Optional transaction</param>
        /// <exception cref="Exception">You must provide a non-null, non-empty dictionary to UpdateManyAsync</exception>
        public static async Task UpdateManyAsync<T>(this T connection, string tableViewName, string idColumn, object idValue, Dictionary<string, object> data, int timeout = 30, DbTransaction transaction = null) where T : DbConnection
        {
            if (data == null && data.Count == 0)
                throw new Exception("You must provide a non-null, non-empty dictionary to UpdateManyAsync");

            var comm = connection.CreateCommand();
            comm.CommandType = CommandType.Text;
            comm.CommandTimeout = timeout;

            var parsLoop = 1;

            var par1 = comm.CreateParameter();
            par1.ParameterName = "@Param1";
            par1.Value = idValue;
            comm.Parameters.Add(par1);

            parsLoop = 2;

            var databaseScript = $"UPDATE {tableViewName} SET ";

            var parsList = new List<String>();

            foreach (var aKey in data.Keys)
            {
                var theValue = data[aKey];

                var aPar = comm.CreateParameter();
                aPar.ParameterName = $"@Param{parsLoop}";
                aPar.Value = theValue;
                comm.Parameters.Add(aPar);

                var setSql = $"{aKey} = {aPar.ParameterName}";

                parsList.Add(setSql);

                parsLoop++;
            }

            databaseScript += string.Join(",", parsList);

            databaseScript += $" WHERE {idColumn} = @Param1";

            comm.CommandText = databaseScript;

            if (transaction != null)
                comm.Transaction = transaction;

            await comm.ExecuteNonQueryAsync();

        }

        /// <summary>
        /// Update multiple data items in the specified tables and columns as an asynchronous operation, without needing datbase script
        /// </summary>
        /// <typeparam name="T">DBConnection sub-class</typeparam>
        /// <param name="connection">The connection object</param>
        /// <param name="tableViewName">Name of the table or view</param>
        /// <param name="whereParams">The where parameter dictionary, string = ColumnName, object = value</param>
        /// <param name="data">The data dictionary, string = ColumnName, object = value</param>
        /// <param name="timeout">The timeout</param>
        /// <param name="transaction">Optional transaction</param>
        /// <exception cref="Exception">You must provide a non-null, non-empty dictionary to UpdateManyAsync</exception>
        public static async Task UpdateManyAsync<T>(this T connection, string tableViewName, Dictionary<string, object> whereParams, Dictionary<string, object> data, int timeout = 30, DbTransaction transaction = null) where T : DbConnection
        {
            if (data == null && data.Count == 0)
                throw new Exception("You must provide a non-null, non-empty dictionary to UpdateManyAsync");

            var comm = connection.CreateCommand();
            comm.CommandType = CommandType.Text;
            comm.CommandTimeout = timeout;

            var parsLoop = 1;

            var databaseScript = $"UPDATE {tableViewName} SET ";

            var parsList = new List<String>();

            foreach (var aKey in data.Keys)
            {
                var theValue = data[aKey];

                var aPar = comm.CreateParameter();
                aPar.ParameterName = $"@Param{parsLoop}";
                aPar.Value = theValue;
                comm.Parameters.Add(aPar);

                var setSql = $"{aKey} = {aPar.ParameterName}";

                parsList.Add(setSql);

                parsLoop++;
            }

            databaseScript += string.Join(",", parsList);

            if (whereParams != null && whereParams.Count > 0)
                databaseScript += comm.BuildWhere(whereParams);

            comm.CommandText = databaseScript;

            if (transaction != null)
                comm.Transaction = transaction;

            await comm.ExecuteNonQueryAsync();

        }


        /// <summary>
        /// Runs the specified database script as an asynchronous operation and returns the whole DataSet
        /// </summary>
        /// <typeparam name="T">Sub-class of DbConnection</typeparam>
        /// <param name="connection">The connection object</param>
        /// <param name="databaseScript">The SQL to execute</param>
        /// <param name="pars">The parameters</param>
        /// <param name="commandType">Type of the command</param>
        /// <param name="timeout">The timeout</param>
        /// <returns>Task&lt;DataSet&gt;.</returns>
        public static async Task<DataSet> QueryDataSetAsync<T>(this T connection, string databaseScript, List<DbParameter> pars = null, CommandType commandType = CommandType.Text, int timeout = 30) where T : DbConnection
        {
            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync();
            }

            var newCmq = connection.CreateCommand();
            newCmq.CommandType = commandType;
            newCmq.CommandText = databaseScript;
            newCmq.CommandTimeout = timeout;

            if (pars != null)
                newCmq.Parameters.AddRange(pars.ToArray());

            var result = await Task.Factory.StartNew<DataSet>(() =>
            {
                try
                {
                    var dt = new DataSet();

                    var dA = connection.CreateDataAdapter();
                    dA.SelectCommand = newCmq;
                    dA.Fill(dt);


                    return dt;
                }
                catch (Exception ex)
                {
                    var message = String.Format("Error occured running the query: {0}", databaseScript);

                    throw new AggregateException(message, ex);
                }

            });


            return result;

        }

        #endregion

        /// <summary>
        /// Creates a data adapter for the specific connection
        /// </summary>
        /// <typeparam name="T">DBConnection sub-class</typeparam>
        /// <param name="connection">The connection objects</param>
        /// <returns>DbDataAdapter.</returns>
        /// <exception cref="ArgumentException">Could not locate factory matching supplied DbConnection;connection</exception>
        public static DbDataAdapter CreateDataAdapter<T>(this T connection) where T : DbConnection
        {
            var factory = DbProviderFactories.GetFactory(connection);

            if (factory == null)
            {
                if (connection is OdbcConnection)
                    factory = DbProviderFactories.GetFactory("System.Data.Odbc");
            }

            if (factory == null)
                throw new ArgumentException("Could not locate factory matching supplied DbConnection", "connection");

            return factory.CreateDataAdapter();
        }

        #region Private Methods

        /// <summary>
        /// Builds the where statement
        /// </summary>
        /// <typeparam name="T">DBConnection sub-class</typeparam>
        /// <param name="command">The command object</param>
        /// <param name="whereParams">The where parameter dictionary, string = ColumnName, object = value</param>
        /// <returns>System.String.</returns>
        private static string BuildWhere<T>(this T command, Dictionary<string, object> whereParams) where T : DbCommand
        {
            var whereSql = " WHERE";
            var parsLoop = 1;

            foreach (var aKey in whereParams.Keys)
            {
                if (parsLoop > 1)
                    whereSql += " AND ";

                var paramName = $"@WhereParam{parsLoop}";

                whereSql += $" {aKey} = {paramName}";

                var par1 = command.CreateParameter();
                par1.ParameterName = paramName;
                par1.Value = whereParams[aKey];
                command.Parameters.Add(par1);

                parsLoop++;
            }



            return whereSql;
        }

        #endregion

    }
}
