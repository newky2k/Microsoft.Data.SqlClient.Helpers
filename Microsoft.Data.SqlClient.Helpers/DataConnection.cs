﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;

namespace Microsoft.Data.SqlClient.Helpers
{
    /// <summary>
    /// DataConnection class using SqlConnection
    /// Implements the <see cref="System.IDisposable" />
    /// </summary>
    /// <seealso cref="System.IDisposable" />
    public class DataConnection : IDisposable
    {
        #region Fields

        /// <summary>
        /// The m connection string
        /// </summary>
        private string mConnString;

        /// <summary>
        /// The connnection
        /// </summary>
        private SqlConnection connnection;
        #endregion

        #region Properties

        /// <summary>
        /// Gets or sets the global timeout override.
        /// </summary>
        /// <value>The global timeout override.</value>
        public static int? GlobalTimeoutOverride { get; set; }

        /// <summary>
        /// Gets or sets the local timeout for the current instance
        /// </summary>
        /// <value>The local timeout override.</value>
        public int? LocalTimeoutOverride { get; set; }

        /// <summary>
        /// Gets or sets the connection string.
        /// </summary>
        /// <value>The connection string.</value>
        public String ConnectionString
        {
            get
            {
                return mConnString;
            }
            set
            {
                mConnString = value;
            }
        }


        /// <summary>
        /// Gets the SqlConnection object for this instance of DataConnection or its sub-class
        /// </summary>
        /// <value>The connection.</value>
        /// <exception cref="Exception">You must specify a connection string</exception>
        public SqlConnection Connection
        {
            get
            {
                if (connnection == null)
                {
                    if (String.IsNullOrWhiteSpace(ConnectionString))
                        throw new Exception("You must specify a connection string");

                    connnection = new SqlConnection(ConnectionString);
                }

                //try and open the connection if its closed
                if (connnection.State != ConnectionState.Open)
                {
                    connnection.Open();
                }

                return connnection;
            }
        }

        #endregion

        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="DataConnection" /> class.
        /// </summary>
        public DataConnection()
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DataConnection" /> class.
        /// </summary>
        /// <param name="connectionString">The connection string to use</param>
        public DataConnection(string connectionString)
        {
            ConnectionString = connectionString;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DataConnection"/> class.
        /// </summary>
        /// <param name="connectionString">The connection string to use</param>
        /// <param name="localTimeoutOverride">The timeout to set on this connection</param>
        public DataConnection(string connectionString, int localTimeoutOverride) : this(connectionString)
        {
            this.LocalTimeoutOverride = localTimeoutOverride;
        }
        #endregion

        #region Private Methods

        /// <summary>
        /// Gets the applicable time out.
        /// </summary>
        /// <param name="timeout">The timeout.</param>
        /// <returns>System.Int32.</returns>
        private int GetTimeOut(int timeout)
        {
            if (GlobalTimeoutOverride.HasValue)
                return GlobalTimeoutOverride.Value;

            if (LocalTimeoutOverride.HasValue)
                return LocalTimeoutOverride.Value;

            return timeout;
        }

        #endregion

        #region Data Methods

        /// <summary>
        /// Starts a database transaction
        /// </summary>
        /// <returns>System.Data.SqlClient.SqlTransaction.</returns>
        public SqlTransaction BeginTransaction()
        {
            return Connection.BeginTransaction();
        }

        /// <summary>
        /// Check to see if the table or view exists
        /// </summary>
        /// <param name="tableViewName">Name of the table or view.</param>
        /// <returns><c>true</c> if exists, <c>false</c> otherwise.</returns>
        public bool DoesTableViewExist(string tableViewName)
        {
            return Connection.DoesTableViewExist(tableViewName);

        }

        /// <summary>
        /// Executes the database script
        /// </summary>
        /// <param name="sSql">The database script to execute</param>
        /// <param name="pars">The parameters</param>
        /// <param name="commandType">Type of the command</param>
        /// <param name="timeout">The timeout</param>
        /// <param name="transaction">Optional transaction</param>
        public void Execute(string sSql, List<DbParameter> pars = null, CommandType commandType = CommandType.Text, int timeout = 30, SqlTransaction transaction = null)
        {
            Connection.Execute(sSql, pars, commandType, GetTimeOut(timeout), transaction);
        }


        /// <summary>
        /// Executes the query and returns the first column of the first row in the result set returned by the query. Additional columns or rows are ignored.
        /// </summary>
        /// <param name="sSql">The database script to execute</param>
        /// <param name="pars">The parameters</param>
        /// <param name="commandType">Type of the command</param>
        /// <param name="timeout">The timeout</param>
        /// <param name="transaction">Optional transaction</param>
        /// <returns>System.Object.</returns>
        public object ExecuteScalar(string sSql, List<DbParameter> pars = null, CommandType commandType = CommandType.Text, int timeout = 30, SqlTransaction transaction = null)
        {
            return Connection.ExecuteScalar(sSql, pars, commandType, GetTimeOut(timeout), transaction);
        }

        /// <summary>
        /// Executes the database script as an asynchronous operation.
        /// </summary>
        /// <param name="sSql">The database script to execute</param>
        /// <param name="pars">The parameters</param>
        /// <param name="commandType">Type of the command</param>
        /// <param name="timeout">The timeout</param>
        /// <param name="transaction">Optional transaction</param>
        public async Task ExecuteAsync(string sSql, List<DbParameter> pars = null, CommandType commandType = CommandType.Text, int timeout = 30, SqlTransaction transaction = null)
        {
            await Connection.ExecuteAsync(sSql, pars, commandType, GetTimeOut(timeout), transaction);
        }

        /// <summary>
        /// Executes the query as an asynchronous operation and returns the first column of the first row in the result set returned by the query. Additional columns or rows are ignored. 
        /// </summary>
        /// <param name="sSql">The database script to execute</param>
        /// <param name="pars">The parameters</param>
        /// <param name="commandType">Type of the command</param>
        /// <param name="timeout">The timeout</param>
        /// <param name="transaction">Optional transaction</param>
        /// <returns>Task&lt;System.Object&gt;.</returns>
        public async Task<object> ExecuteScalarAsync(string sSql, List<DbParameter> pars = null, CommandType commandType = CommandType.Text, int timeout = 30, SqlTransaction transaction = null)
        {
            return await Connection.ExecuteScalarAsync(sSql, pars, commandType, GetTimeOut(timeout), transaction);
        }

        /// <summary>
        /// Executes an insert as an asynchronous operation and returns the first column of the first row as an int
        /// </summary>
        /// <param name="sSql">The database script to execute</param>
        /// <param name="pars">The parameters</param>
        /// <param name="commandType">Type of the command</param>
        /// <param name="timeout">The timeout</param>
        /// <param name="transaction">Optional transaction</param>
        /// <returns>Task&lt;System.Int32&gt;.</returns>
        public async Task<int> ExecuteInsertAsync(string sSql, List<DbParameter> pars = null, CommandType commandType = CommandType.Text, int timeout = 30, SqlTransaction transaction = null)
        {

            return await Connection.ExecuteInsertAsync(sSql, pars, commandType, GetTimeOut(timeout), transaction);

        }

        /// <summary>
        /// Runs the specified database script and returns a DataTable with the results
        /// </summary>
        /// <param name="sSql">The database script to execute</param>
        /// <param name="pars">The parameters</param>
        /// <param name="commandType">Type of the command</param>
        /// <param name="timeout">The timeout</param>
        /// <returns>DataTable.</returns>
        public DataTable Query(string sSql, List<DbParameter> pars = null, CommandType commandType = CommandType.Text, int timeout = 30)
        {
            return Connection.Query(sSql, pars, commandType, GetTimeOut(timeout));
        }

        /// <summary>
        /// Runs the specified database script as an asynchronous operation and returns a DataTable with the results
        /// </summary>
        /// <param name="sSql">The database script to execute</param>
        /// <param name="pars">The parameters</param>
        /// <param name="commandType">Type of the command</param>
        /// <param name="timeout">The timeout</param>
        /// <returns>Task&lt;DataTable&gt;.</returns>
        public async Task<DataTable> QueryAsync(string sSql, List<DbParameter> pars = null, CommandType commandType = CommandType.Text, int timeout = 30)
        {
            return await Connection.QueryAsync(sSql, pars, commandType, GetTimeOut(timeout));

        }


        /// <summary>
        /// Checks to see if the record already exists as an asynchronous operation
        /// </summary>
        /// <param name="tableViewName">Name of the table or view</param>
        /// <param name="columnName">Name of the column to compare</param>
        /// <param name="value">The value to compare</param>
        /// <param name="timeout">The timeout</param>
        /// <returns>Task&lt;System.Boolean&gt;.</returns>
        public async Task<bool> ExistsAsync(string tableViewName, string columnName, object value, int timeout = 30)
        {
            return await Connection.ExistsAsync(tableViewName, columnName, value, GetTimeOut(timeout));
        }


        /// <summary>
        /// Checks to see if the record already exists as an asynchronous operation
        /// </summary>
        /// <param name="tableViewName">Name of the table or view</param>
        /// <param name="columnName">Name of the column</param>
        /// <param name="whereParams">The where parameter dictionary, string = ColumnName, object = value</param>
        /// <param name="timeout">The timeout</param>
        /// <returns>Task&lt;System.Boolean&gt;.</returns>
        public async Task<bool> ExistsAsync(string tableViewName, string columnName, Dictionary<string, object> whereParams, int timeout = 30)
        {
            return await Connection.ExistsAsync(tableViewName, columnName, whereParams, GetTimeOut(timeout));
        }


        /// <summary>
        /// Runs the specified database script as an asynchronous operation and returns the whole DataSet
        /// </summary>
        /// <param name="sSql">The SQL to execute</param>
        /// <param name="pars">The parameters</param>
        /// <param name="commandType">Type of the command</param>
        /// <param name="timeout">The timeout</param>
        /// <returns>Task&lt;DataSet&gt;.</returns>
        public async Task<DataSet> QueryDataSetAsync(string sSql, List<DbParameter> pars = null, CommandType commandType = CommandType.Text, int timeout = 30)
        {
            return await Connection.QueryDataSetAsync(sSql, pars, commandType, GetTimeOut(timeout));

        }

        /// <summary>
        /// Gets the DataTable schema from table.
        /// </summary>
        /// <param name="tableViewName">Name of the table or view</param>
        /// <param name="transaction">Optional transaction</param>
        /// <returns>DataTable</returns>
        public DataTable GetDataTableSchemaFromTable(string tableViewName, DbTransaction transaction = null)
        {
            return Connection.GetDataTableSchemaFromTable(tableViewName, transaction);
        }

        /// <summary>
        /// Inserts one data item as an asynchronous operation, without needing database script 
        /// </summary>
        /// <param name="tableViewName">Name of the table or view</param>
        /// <param name="idColumnName">The identifier column name</param>
        /// <param name="idValue">The identifier value name</param>
        /// <param name="valueColumnName">The value column name</param>
        /// <param name="value">The value to insert</param>
        /// <param name="timeout">The timeout</param>
        /// <param name="transaction">Optional transaction</param>
        public async Task InsertOneAsync(string tableViewName, string idColumnName, object idValue, string valueColumnName, object value, int timeout = 30, SqlTransaction transaction = null)
        {
            await Connection.InsertOneAsync(tableViewName, idColumnName, idValue, valueColumnName, value, GetTimeOut(timeout), transaction);
        }

        /// <summary>
        /// Inserts one data item as an asynchronous operation, without needing database script 
        /// </summary>
        /// <param name="tableViewName">Name of the table or view</param>
        /// <param name="idColumnName">The identifier column name</param>
        /// <param name="idValue">The identifier value name</param>
        /// <param name="valueColumnName">The value column name</param>
        /// <param name="value">The value to insert</param>
        /// <param name="timeout">The timeout</param>
        /// <param name="transaction">Optional transaction</param>
        public async Task UpdateOneAsync(string tableViewName, string idColumnName, object idValue, string valueColumnName, object value, int timeout = 30, SqlTransaction transaction = null)
        {
            await Connection.UpdateOneAsync(tableViewName, idColumnName, idValue, valueColumnName, value, GetTimeOut(timeout), transaction);
        }

        /// <summary>
        /// Update multiple data items in the specified tables and columns as an asynchronous operation, without needing datbase script
        /// </summary>
        /// <param name="tableViewName">Name of the table or view</param>
        /// <param name="idColumnName">The identifier column</param>
        /// <param name="idValue">The identifier value</param>
        /// <param name="data">The data dictionary, string = ColumnName, object = value</param>
        /// <param name="timeout">The timeout</param>
        /// <param name="transaction">Optional transaction</param>
        /// <exception cref="Exception">You must provide a non-null, non-empty dictionary to UpdateManyAsync</exception>
        public async Task UpdateManyAsync(string tableViewName, string idColumnName, object idValue, Dictionary<string, object> data, int timeout = 30, SqlTransaction transaction = null)
        {
            await Connection.UpdateManyAsync(tableViewName, idColumnName, idValue, data, GetTimeOut(timeout), transaction);
        }

        /// <summary>
        /// Update multiple data items in the specified tables and columns as an asynchronous operation, without needing datbase script
        /// </summary>
        /// <param name="tableViewName">Name of the table or view</param>
        /// <param name="whereParams">The where parameter dictionary, string = ColumnName, object = value</param>
        /// <param name="data">The data dictionary, string = ColumnName, object = value</param>
        /// <param name="timeout">The timeout</param>
        /// <param name="transaction">Optional transaction</param>
        /// <exception cref="Exception">You must provide a non-null, non-empty dictionary to UpdateManyAsync</exception>
        public async Task UpdateManyAsync(string tableViewName, Dictionary<string, object> whereParams, Dictionary<string, object> data, int timeout = 30, SqlTransaction transaction = null)
        {
            await Connection.UpdateManyAsync(tableViewName, whereParams, data, GetTimeOut(timeout), transaction);
        }

        /// <summary>
        /// Inserts multiple data items into the specified tables and columns as an asynchronous operation, without needing database script
        /// </summary>
        /// <param name="tableViewName">Name of the table or view</param>
        /// <param name="idColumnName">The identifier column</param>
        /// <param name="idValue">The identifier value</param>
        /// <param name="data">The data dictionary, string = ColumnName, object = value</param>
        /// <param name="timeout">The timeout</param>
        /// <param name="transaction">Optional transaction</param>
        /// <exception cref="Exception">You must provide a non-null, non-empty dictionary to InsertManyAsync</exception>
        public async Task InsertManyAsync(string tableViewName, string idColumnName, object idValue, Dictionary<string, object> data, int timeout = 30, SqlTransaction transaction = null)
        {
            await Connection.InsertManyAsync(tableViewName, idColumnName, idValue, data, GetTimeOut(timeout), transaction);
        }

        /// <summary>
        /// Inserts multiple data items into the specified tables and columns as an asynchronous operation, without needing datbase script
        /// </summary>
        /// <param name="tableViewName">Name of the table or view</param>
        /// <param name="data">The data dictionary, string = ColumnName, object = value</param>
        /// <param name="timeout">The timeout</param>
        /// <param name="transaction">Optional transaction</param>
        /// <exception cref="Exception">You must provide a non-null, non-empty dictionary to InsertManyAsync</exception>
        public async Task InsertManyAsync(string tableViewName, Dictionary<string, object> data, int timeout = 30, SqlTransaction transaction = null)
        {
            await Connection.InsertManyAsync(tableViewName, data, GetTimeOut(timeout), transaction);
        }

        /// <summary>
        /// Inserts multiple data items into the specified tables and columns without needing database script
        /// </summary>
        /// <param name="tableViewName">Name of the table or view</param>
        /// <param name="data">The data dictionary, string = ColumnName, object = value</param>
        /// <param name="timeout">The timeout</param>
        /// <param name="transaction">Optional transaction</param>
        /// <exception cref="Exception">You must provide a non-null, non-empty dictionary to InsertMany</exception>
        public void InsertMany(string tableViewName, Dictionary<string, object> data, int timeout = 30, SqlTransaction transaction = null)
        {
            Connection.InsertMany(tableViewName, data, GetTimeOut(timeout), transaction);
        }

        /// <summary>
        /// Execute a bulk insert, using SqlBulkCopy,  as an asynchronous operation.
        /// </summary>
        /// <param name="tableName">Name of the table</param>
        ///<param name="data">The data dictionary, string = ColumnName, object = value</param>
        /// <param name="transaction">The SQL transaction.</param>
        /// <param name="timeout">The timeout.</param>
        /// <exception cref="Exception">
        /// DbConnection.GetDataTableSchemaFromTable could not load the schema for {tableViewName} during a call to BulkInsertAsync
        /// or
        /// </exception>
        public async Task BulkInsertAsync(string tableName, List<Dictionary<string, object>> data, SqlTransaction transaction, int timeout = 30)
        {
            //load the datatable
            var dtTemplate = Connection.GetDataTableSchemaFromTable("JobExpenses", transaction);

            if (dtTemplate == null)
                throw new Exception($"DbConnection.GetDataTableSchemaFromTable could not load the schema for {tableName} during a call to BulkInsertAsync");


            //remove the index one
            dtTemplate.Columns.RemoveAt(0);

            //work through each record and assign the columns of data to the items from the dictionary
            foreach (Dictionary<string, object> aEntry in data)
            {
                var dtRow = dtTemplate.NewRow();

                foreach (var aKey in aEntry.Keys)
                {
                    if (!dtTemplate.Columns.Contains(aKey))
                        throw new Exception($"{tableName} does not contain a column called {aKey}.  Check the case of the column name");

                    dtRow[aKey] = aEntry[aKey];

                }

                dtTemplate.Rows.Add(dtRow);

            }

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection, SqlBulkCopyOptions.Default, transaction))
            {
                bulkCopy.BatchSize = 250;
                bulkCopy.DestinationTableName = tableName;

                // Write from the source to the destination.
                // This should fail with a duplicate key error.
                await bulkCopy.WriteToServerAsync(dtTemplate);
            }
        }
        #endregion

        #region Methods

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public virtual void Dispose()
        {
            if (connnection != null)
            {
                connnection.Dispose();

                connnection = null;
            }
        }

        #endregion

        #region Static Methods

        /// <summary>
        /// Gets the time out applicable global timeout.
        /// </summary>
        /// <param name="timeout">The suggested timeout</param>
        /// <returns>System.Int32.</returns>
        private static int GetGlobalTimeOut(int timeout)
        {
            if (GlobalTimeoutOverride.HasValue)
                return GlobalTimeoutOverride.Value;

            return timeout;
        }

        /// <summary>
        /// Determines whether this instance can connect the server using the specified connection string.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="timeout">The timeout.</param>
        /// <param name="throwException">if set to <c>true</c> [throw exception].</param>
        /// <returns><c>true</c> if this instance can connect the specified connection string; otherwise, <c>false</c>.</returns>
        /// <exception cref="Exception"></exception>
        /// <exception cref="System.Exception"></exception>
        public static bool CanConnect(string connectionString, int timeout = 10, bool throwException = false)
        {
            try
            {
                //
                var conString = connectionString;

                if (!conString.Contains("Timeout"))
                {
                    if (!conString.EndsWith(";"))
                        conString += ";";

                    conString += String.Format("Connection Timeout={0};", GetGlobalTimeOut(timeout).ToString());
                }

                using (var mSqlConn = new SqlConnection(conString))
                {
                    mSqlConn.Open();

                    if (mSqlConn.State != ConnectionState.Open)
                    {
                        return false;
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                if (throwException == true)
                {
                    throw new Exception(String.Format("Unable to connect to database {0}", Environment.NewLine + Environment.NewLine + connectionString, ex));
                }

            }

            return false;
        }

        /// <summary>
        /// Determines whether this instance can connect the server using the specified connection string, as an asynchronous operation
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="timeout">The timeout.</param>
        /// <param name="throwException">if set to <c>true</c> [throw exception].</param>
        /// <returns>Task&lt;System.Boolean&gt;.</returns>
        /// <exception cref="Exception"></exception>
        public static async Task<bool> CanConnectAsync(string connectionString, int timeout = 10, bool throwException = false)
        {
            try
            {
                //
                var conString = connectionString;

                if (!conString.Contains("Timeout"))
                {
                    if (!conString.EndsWith(";"))
                        conString += ";";

                    conString += String.Format("Connection Timeout={0};", GetGlobalTimeOut(timeout).ToString());
                }

                using (var mSqlConn = new SqlConnection(conString))
                {
                    await mSqlConn.OpenAsync();

                    if (mSqlConn.State != ConnectionState.Open)
                    {
                        return false;
                    }
                }

                return true;
            }
            catch (Exception ex)
            {
                if (throwException == true)
                {
                    throw new Exception(String.Format("Unable to connect to database {0}", Environment.NewLine + Environment.NewLine + connectionString, ex));
                }

            }

            return false;
        }



        #endregion
    }
}
