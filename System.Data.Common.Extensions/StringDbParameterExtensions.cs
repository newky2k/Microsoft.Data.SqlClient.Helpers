using System;
using System.Collections.Generic;
using System.Text;

namespace System
{
    public static class StringDbParameterExtensions
    {
        public static object AsValueOrDbNull(this string target)
        {
            if (String.IsNullOrWhiteSpace(target))
                return DBNull.Value;

            return target;
        }

        public static object AsValueOrDefault(this string target, string defaultValue)
        {
            if (String.IsNullOrWhiteSpace(target))
                return defaultValue;

            return target;
        }
    }
}
