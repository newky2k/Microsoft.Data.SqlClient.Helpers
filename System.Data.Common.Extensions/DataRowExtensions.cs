using System;
using System.Collections.Generic;
using System.Text;

namespace System.Data
{
    public static class DataRowExtensions
    {
        public static void WhenValid(this DataRow row, string columnName, Action<object> action)
        {
            if (!row.Table.Columns.Contains(columnName))
                return;

            if (row[columnName] == DBNull.Value)
                return;

            action(row[columnName]);

        }

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
