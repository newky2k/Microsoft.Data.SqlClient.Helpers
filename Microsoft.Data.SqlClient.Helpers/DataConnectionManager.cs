using System;
using System.Collections.Generic;
using System.Text;

namespace Microsoft.Data.SqlClient.Helpers
{
    public class DataConnectionManager
    {
        /// <summary>
        /// Gets the instance.
        /// </summary>
        /// <value>
        /// The instance.
        /// </value>
        public static DataConnectionManager Instance => mInstance.Value;

        public static string GetConnectionString(string key)
        {
            return Instance.ConnectionString(key);
        }



        #region Fields
        private static Lazy<DataConnectionManager> mInstance = new Lazy<DataConnectionManager>(()=> new DataConnectionManager());
        private Func<string, String> mConnectionStringLoader;

        private Dictionary<string, string> _overrides = new Dictionary<string, string>();
        private Dictionary<string, string> _connectionStrings = new Dictionary<string, string>();

        #endregion

        #region Properties

        /// <summary>
        /// Gets the connection string loader.
        /// </summary>
        /// <value>The connection string loader.</value>
        public Func<String, String> ConnectionStringLoader
        {
            get
            {
                return mConnectionStringLoader;
            }
        }

        /// <summary>
        /// Gets the default connection strings.
        /// </summary>
        /// <value>
        /// The connection strings.
        /// </value>
        private Dictionary<string, String> ConnectionStrings { get => _connectionStrings; set => _connectionStrings = value; }

        #endregion

        #region Methods

        /// <summary>
        /// Sets the connection loader.
        /// </summary>
        /// <param name="function">The function.</param>
        public void SetConnectionStringLoader(Func<String, String> function)
        {
            mConnectionStringLoader = function;
        }

        public void AddOverride(string originalValue, string overrideValue)
        {
            if (_overrides.ContainsKey(originalValue))
            {
                if (!string.IsNullOrWhiteSpace(overrideValue))
                    _overrides[originalValue] = overrideValue;
                else
                    _overrides.Remove(originalValue);
            }
            else
            {
                if (!string.IsNullOrWhiteSpace(overrideValue))
                    _overrides.Add(originalValue, overrideValue);
            }
        }
        /// <summary>
        /// Connections the string.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns></returns>
        public string ConnectionString(String key)
        {
            var conString = String.Empty;

            var theKey = (_overrides.ContainsKey(key)) ? _overrides[key] : key;

            if (mConnectionStringLoader == null)
            {
                if (ConnectionStrings.ContainsKey(theKey))
                    conString = ConnectionStrings[theKey];
            }
            else
            {
                conString = mConnectionStringLoader(theKey);
            }

            if (String.IsNullOrWhiteSpace(conString))
            {
                if (ConnectionStrings.ContainsKey(theKey))
                    return ConnectionStrings[theKey];

                throw new Exception("Connection string could not be found");
            }

            return conString;
        }

        #endregion

    }
}
