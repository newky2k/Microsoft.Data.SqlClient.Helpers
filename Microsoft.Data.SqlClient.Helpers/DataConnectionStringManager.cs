using System;
using System.Collections.Generic;
using System.Text;

namespace Microsoft.Data.SqlClient.Helpers
{
    /// <summary>
    /// Data Connection String Manager.
    /// </summary>
    public class DataConnectionStringManager
    {
        /// <summary>
        /// Gets the instance.
        /// </summary>
        /// <value>The instance.</value>
        private static DataConnectionStringManager Instance => _instance.Value;

        /// <summary>
        /// Gets or sets the connection string loader function, for returning the correct connection string
        /// </summary>
        /// <value>The connection string loader.</value>
        /// <example>
        /// <code>
        /// DataConnectionStringManager.Instance.ConnectionStringLoader = (name) =>
        /// {
        ///     return ConfigurationManager.ConnectionStrings[name].ConnectionString;
        /// };
        /// </code>
        /// </example>
        public static Func<string, string> ConnectionStringLoader { get; set; }


        #region Fields

        /// <summary>
        /// The singleton instance
        /// </summary>
        private static Lazy<DataConnectionStringManager> _instance = new Lazy<DataConnectionStringManager>(()=> new DataConnectionStringManager());

        /// <summary>
        /// The overriden connection strings
        /// </summary>
        private Dictionary<string, string> _overrides = new Dictionary<string, string>();

        #endregion

        #region Properties

        /// <summary>
        /// Gets or sets the connection strings.
        /// </summary>
        /// <value>The registered connection strings</value>
        private Dictionary<string, string> ConnectionStrings { get; set; } = new Dictionary<string, string>();

        #endregion

        #region Methods

        /// <summary>
        /// Gets the connection string.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <returns>System.String.</returns>
        public static string GetConnectionString(string key)
        {
            return ConnectionString(key);
        }

        /// <summary>
        /// Add or Update a connection string
        /// </summary>
        /// <param name="key">The connection string key</param>
        /// <param name="connectionString">The connection string.</param>
        public static void SetConnectionString(string key, string connectionString)
        {
            if (Instance.ConnectionStrings.ContainsKey(key))
            {
                Instance.ConnectionStrings[key] = connectionString;
            }
            else
            {
                Instance.ConnectionStrings.Add(key, connectionString);
            }
        }

        /// <summary>
        /// Adds an override for the connection string for the specified key
        /// </summary>
        /// <param name="key">The connection string key</param>
        /// <param name="overrideValue">The new connection string</param>
        public static void AddOverride(string key, string overrideValue)
        {
            if (Instance._overrides.ContainsKey(key))
            {
                if (!string.IsNullOrWhiteSpace(overrideValue))
                    Instance._overrides[key] = overrideValue;
                else
                    Instance._overrides.Remove(key);
            }
            else
            {
                if (!string.IsNullOrWhiteSpace(overrideValue))
                    Instance._overrides.Add(key, overrideValue);
            }
        }

        /// <summary>
        /// Get the connection string for the specified key.  Will call the ConnectionStringLoader first, then check for overrides and then any registered connections strings in ConnectionStrings
        /// </summary>
        /// <param name="key">The connection string key</param>
        /// <returns>System.String.</returns>
        /// <exception cref="Exception">Connection string could not be found</exception>
        private static string ConnectionString(string key)
        {
            var conString = String.Empty;

            var theKey = (Instance._overrides.ContainsKey(key)) ? Instance._overrides[key] : key;

            if (ConnectionStringLoader != null)
            {
               conString = ConnectionStringLoader(theKey);
            }

            if (string.IsNullOrWhiteSpace(conString))
            {
                if (Instance._overrides.ContainsKey(theKey))
                    return Instance._overrides[theKey];
                else if (Instance.ConnectionStrings.ContainsKey(theKey))
                    return Instance.ConnectionStrings[theKey];

                throw new Exception($"Connection string for key: {key} could not be found");
            }

            return conString;
        }

        #endregion

    }
}
