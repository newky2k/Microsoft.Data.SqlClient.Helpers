using System;
using System.Collections.Generic;
using System.Text;

namespace System
{
    /// <summary>
    /// DbParameter String Extensions.
    /// </summary>
    public static class StringDbParameterExtensions
    {
        /// <summary>
        /// Return a DBNull.Value if the string is empty, null or whitespace or the value otherwise
        /// </summary>
        /// <param name="target">The target.</param>
        /// <returns>System.Object.</returns>
        public static object AsValueOrDbNull(this string target)
        {
            if (String.IsNullOrWhiteSpace(target))
                return DBNull.Value;

            return target;
        }

        /// <summary>
        /// Return the default value if the string is empty, null or whitespace or the value otherwise
        /// </summary>
        /// <param name="target">The target.</param>
        /// <param name="defaultValue">The default value.</param>
        /// <returns>System.Object.</returns>
        public static object AsValueOrDefault(this string target, string defaultValue)
        {
            if (String.IsNullOrWhiteSpace(target))
                return defaultValue;

            return target;
        }
    }
}
