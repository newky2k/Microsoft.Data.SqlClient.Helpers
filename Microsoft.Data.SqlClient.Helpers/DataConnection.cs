using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;

namespace Microsoft.Data.SqlClient.Helpers
{
    public class DataConnection : IDisposable
    {
        #region Fields

        private string mConnString;

        private SqlConnection connnection;
        #endregion

        #region Properties

        public static int? GlobalTimeoutOverride { get; set; }

        public int? LocalTimeoutOverride { get; set; }

        /// <summary>
        /// Gets or sets the connection string.
        /// </summary>
        /// <value>
        /// The connection string.
        /// </value>
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
        /// Gets the connection for this instance of DataConnection or its sub-class
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
        /// Initializes a new instance of the <see cref="DataConnection"/> class.
        /// </summary>
        public DataConnection()
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DataConnection"/> class.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        public DataConnection(string connectionString)
        {
            ConnectionString = connectionString;
        }

        public DataConnection(string connectionString, int localTimeoutOverride) : this(connectionString)
        {
            this.LocalTimeoutOverride = localTimeoutOverride;
        }
        #endregion

        #region Private Methods

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

        public bool DoesTableViewExist(string tableViewName)
        {
            return Connection.DoesTableViewExist(tableViewName);

        }

        /// <summary>
        /// Executes the sql script
        /// </summary>
        /// <param name="sSql">The sql script</param>
        /// <param name="pars">The parameters.</param>
        public void Execute(string sSql, List<DbParameter> pars = null, CommandType commandType = CommandType.Text, int timeout = 30, SqlTransaction transaction = null)
        {
            Connection.Execute(sSql, pars, commandType, GetTimeOut(timeout), transaction);
        }

        /// <summary>
        /// Executes the query, and returns the first column of the first row in the result set returned by the query. Additional columns or rows are ignored.
        /// </summary>
        /// <param name="sSql">The SQL string to execute</param>
        /// <param name="pars">The parameters</param>
        /// <param name="commandType">Type of the command.</param>
        /// <param name="timeout">The timeout.</param>
        /// <returns>System.Object.</returns>
        public object ExecuteScalar(string sSql, List<DbParameter> pars = null, CommandType commandType = CommandType.Text, int timeout = 30, SqlTransaction transaction = null)
        {
            return Connection.ExecuteScalar(sSql, pars, commandType, GetTimeOut(timeout), transaction);
        }

        /// <summary>
        /// Executes the sql script
        /// </summary>
        /// <param name="sSql">The sql script</param>
        /// <param name="pars">The parameters.</param>
        /// <returns></returns>
        public async Task ExecuteAsync(string sSql, List<DbParameter> pars = null, CommandType commandType = CommandType.Text, int timeout = 30, SqlTransaction transaction = null)
        {
            await Connection.ExecuteAsync(sSql, pars, commandType, GetTimeOut(timeout), transaction);
        }

        /// <summary>
        /// Executes the query asynchronously, and returns the first column of the first row in the result set returned by the query. Additional columns or rows are ignored.
        /// </summary>
        /// <param name="sSql">The s SQL.</param>
        /// <param name="pars">The pars.</param>
        /// <param name="timeout">The timeout.</param>
        /// <returns>Task&lt;System.Object&gt;.</returns>
        public async Task<object> ExecuteScalarAsync(string sSql, List<DbParameter> pars = null, CommandType commandType = CommandType.Text, int timeout = 30, SqlTransaction transaction = null)
        {
            return await Connection.ExecuteScalarAsync(sSql, pars, commandType, GetTimeOut(timeout), transaction);
        }

        /// <summary>
        /// Execute insert as an asynchronously operation and return the new record Id.  
        /// </summary>
        /// <param name="sSql">The s SQL.</param>
        /// <param name="pars">The pars.</param>
        /// <param name="timeout">The timeout.</param>
        /// <returns>Task&lt;System.Int32&gt;.</returns>
        public async Task<int> ExecuteInsertAsync(string sSql, List<DbParameter> pars = null, CommandType commandType = CommandType.Text, int timeout = 30, SqlTransaction transaction = null)
        {
            var res = await ExecuteScalarAsync(sSql, pars, commandType, GetTimeOut(timeout), transaction);

            return Convert.ToInt32(res);

        }

        /// <summary>
        /// Queries the specified s SQL.
        /// </summary>
        /// <param name="sSql">The s SQL.</param>
        /// <param name="pars">The pars.</param>
        /// <returns></returns>
        public DataTable Query(string sSql, List<DbParameter> pars = null, CommandType commandType = CommandType.Text, int timeout = 30)
        {

            return Connection.Query(sSql, pars, commandType, GetTimeOut(timeout));
        }

        /// <summary>
        /// Queries the data connection asynchronously.
        /// </summary>
        /// <param name="sSql">The s SQL.</param>
        /// <param name="pars">The pars.</param>
        /// <returns></returns>
        public async Task<DataTable> QueryAsync(string sSql, List<DbParameter> pars = null, CommandType commandType = CommandType.Text, int timeout = 30)
        {
            return await Connection.QueryAsync(sSql, pars, commandType, GetTimeOut(timeout));

        }


        /// <summary>
        /// Checks to see if the record exists
        /// </summary>
        /// <param name="tableViewName">Name of the table view.</param>
        /// <param name="columnName">Name of the column.</param>
        /// <param name="value">The value.</param>
        /// <param name="timeout">The timeout.</param>
        /// <returns>Task&lt;System.Boolean&gt;.</returns>
        public async Task<bool> ExistsAsync(string tableViewName, string columnName, object value, int timeout = 30)
        {
            return await Connection.ExistsAsync(tableViewName, columnName, value, GetTimeOut(timeout));
        }


        /// <summary>
        /// Checks to see if the record exists
        /// </summary>
        /// <param name="tableViewName">Name of the table view.</param>
        /// <param name="columnName">Name of the column.</param>
        /// <param name="whereParams">The where parameters.</param>
        /// <param name="timeout">The timeout.</param>
        /// <returns>Task&lt;System.Boolean&gt;.</returns>
        public async Task<bool> ExistsAsync(string tableViewName, string columnName, Dictionary<string, object> whereParams, int timeout = 30)
        {
            return await Connection.ExistsAsync(tableViewName, columnName, whereParams, GetTimeOut(timeout));
        }


        /// <summary>
        /// query data set as an asynchronous operation.
        /// </summary>
        /// <param name="sSql">The s SQL.</param>
        /// <param name="pars">The pars.</param>
        /// <param name="commandType">Type of the command.</param>
        /// <param name="timeout">The timeout.</param>
        /// <returns>Task&lt;DataSet&gt;.</returns>
        public async Task<DataSet> QueryDataSetAsync(string sSql, List<DbParameter> pars = null, CommandType commandType = CommandType.Text, int timeout = 30)
        {
            return await Connection.QueryDataSetAsync(sSql, pars, commandType, GetTimeOut(timeout));

        }

        public DataTable GetDataTableSchemaFromTable(string tableViewName, DbTransaction transaction = null)
        {
            return Connection.GetDataTableSchemaFromTable(tableViewName, transaction);
        }

        public async Task InsertOneAsync(string tableViewName, string idColumnName, object idValue, string valueColumnName, object value, int timeout = 30, SqlTransaction transaction = null)
        {
            await Connection.InsertOneAsync(tableViewName, idColumnName, idValue, valueColumnName, value, GetTimeOut(timeout), transaction);
        }

        public async Task UpdateOneAsync(string tableViewName, string idColumnName, object idValue, string valueColumnName, object value, int timeout = 30, SqlTransaction transaction = null)
        {
            await Connection.UpdateOneAsync(tableViewName, idColumnName, idValue, valueColumnName, value, GetTimeOut(timeout), transaction);
        }

        public async Task UpdateManyAsync(string tableViewName, string idColumnName, object idValue, Dictionary<string, object> data, int timeout = 30, SqlTransaction transaction = null)
        {
            await Connection.UpdateManyAsync(tableViewName, idColumnName, idValue, data, GetTimeOut(timeout), transaction);
        }

        public async Task UpdateManyAsync(string tableViewName, Dictionary<string, object> whereParams, Dictionary<string, object> data, int timeout = 30, SqlTransaction transaction = null)
        {
            await Connection.UpdateManyAsync(tableViewName, whereParams, data, GetTimeOut(timeout), transaction);
        }

        /// <summary>
        /// Insert many columns of data into the specificed table
        /// </summary>
        /// <param name="tableViewName">Name of the table view.</param>
        /// <param name="idColumnName">Name of the identifier column.</param>
        /// <param name="idValue">The identifier value.</param>
        /// <param name="data">The data.</param>
        /// <param name="timeout">The timeout.</param>
        /// <param name="transaction">The transaction.</param>
        /// <returns>Task.</returns>
        public async Task InsertManyAsync(string tableViewName, string idColumnName, object idValue, Dictionary<string, object> data, int timeout = 30, SqlTransaction transaction = null)
        {
            await Connection.InsertManyAsync(tableViewName, idColumnName, idValue, data, GetTimeOut(timeout), transaction);
        }

        /// <summary>
        /// Insert many columns of data into the specificed table
        /// </summary>
        /// <param name="tableViewName">Name of the table view.</param>
        /// <param name="data">The data.</param>
        /// <param name="timeout">The timeout.</param>
        /// <param name="transaction">The transaction.</param>
        /// <returns>Task.</returns>
        public async Task InsertManyAsync(string tableViewName, Dictionary<string, object> data, int timeout = 30, SqlTransaction transaction = null)
        {
            await Connection.InsertManyAsync(tableViewName, data, GetTimeOut(timeout), transaction);
        }


        /// <summary>
        /// Insert many columns of data into the specificed table
        /// </summary>
        /// <param name="tableViewName">Name of the table view.</param>
        /// <param name="data">The data.</param>
        /// <param name="timeout">The timeout.</param>
        /// <param name="transaction">The transaction.</param>
        /// <returns>Task.</returns>
        public void InsertMany(string tableViewName, Dictionary<string, object> data, int timeout = 30, SqlTransaction transaction = null)
        {
            Connection.InsertMany(tableViewName, data, GetTimeOut(timeout), transaction);
        }

        public async Task BulkInsertAsync(string tableViewName, List<Dictionary<string, object>> data, SqlTransaction transaction, int timeout = 30)
        {
            //load the datatable
            var dtTemplate = Connection.GetDataTableSchemaFromTable("JobExpenses", transaction);

            if (dtTemplate == null)
                throw new Exception($"DbConnection.GetDataTableSchemaFromTable could not load the schema for {tableViewName} during a call to BulkInsertAsync");


            //remove the index one
            dtTemplate.Columns.RemoveAt(0);

            //work through each record and assign the columns of data to the items from the dictionary
            foreach (Dictionary<string, object> aEntry in data)
            {
                var dtRow = dtTemplate.NewRow();

                foreach (var aKey in aEntry.Keys)
                {
                    if (!dtTemplate.Columns.Contains(aKey))
                        throw new Exception($"{tableViewName} does not contain a column called {aKey}.  Check the case of the column name");

                    dtRow[aKey] = aEntry[aKey];

                }

                dtTemplate.Rows.Add(dtRow);

            }

            using (SqlBulkCopy bulkCopy = new SqlBulkCopy(Connection, SqlBulkCopyOptions.Default, transaction))
            {
                bulkCopy.BatchSize = 250;
                bulkCopy.DestinationTableName = tableViewName;

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

        private static int GetTimeOutStatic(int timeout)
        {
            if (GlobalTimeoutOverride.HasValue)
                return GlobalTimeoutOverride.Value;

            return timeout;
        }

        /// <summary>
        /// Determines whether this instance can connect the specified connection string.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="timeout">The timeout.</param>
        /// <param name="throwException">if set to <c>true</c> [throw exception].</param>
        /// <returns></returns>
        /// <exception cref="System.Exception"></exception>
        public static bool CanConnect(string connectionString, int timeout = 10, bool throwException = false)
        {
            try
            {
                //
                var conString = connectionString;

                if (!conString.Contains("Timeout"))
                {
                    conString += String.Format("Connection Timeout={0};", GetTimeOutStatic(timeout).ToString());
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
        /// Determines whether this instance [can connect asynchronous] the specified connection string.
        /// </summary>
        /// <param name="connectionString">The connection string.</param>
        /// <param name="timeout">The timeout.</param>
        /// <param name="throwException">if set to <c>true</c> [throw exception].</param>
        /// <returns></returns>
        public static async Task<bool> CanConnectAsync(string connectionString, int timeout = 10, bool throwException = false)
        {
            try
            {
                //
                var conString = connectionString;

                if (!conString.Contains("Timeout"))
                {
                    conString += String.Format("Connection Timeout={0};", GetTimeOutStatic(timeout).ToString());
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
