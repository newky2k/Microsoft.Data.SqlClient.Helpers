using System;
using System.Collections.Generic;
using System.Text;

namespace System.Data
{
    public static class DataRowExtensions
    {
        /// <summary>When the value is not null or empty call the action and pass the column.</summary>
        /// <param name="row">The row of data to work on</param>
        /// <param name="columnName">The name of the column</param>
        /// <param name="action">The action to perform if valid</param>
        public static void WhenValid(this DataRow row, string columnName, Action<object> action)
        {
            if (!row.Table.Columns.Contains(columnName))
                return;

            if (row[columnName] == DBNull.Value)
                return;

            action(row[columnName]);

        }

        /// <summary>
        /// When the value is not null or empty set the old value to be the new value
        /// </summary>
        /// <param name="row">The row of data to work on</param>
        /// <param name="columnName">The name of the column</param>
        /// <param name="oldValue">The old value</param>
        /// <param name="newValue">The new value</param>
        public static void WhenValid(this DataRow row, string columnName, object oldValue, object newValue)
        {
            if (!row.Table.Columns.Contains(columnName))
                return;

            if (row[columnName] == DBNull.Value)
                return;

            oldValue = newValue;

        }
    }
}
