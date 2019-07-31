using System;
using System.Collections.Generic;
using System.Text;

namespace System
{
    public static class DateDbParameterExtensions
    {
        /// <summary>
        /// Returns the DateTime?.Value or DbNull.Value if there is no value
        /// </summary>
        /// <param name="target">The DateTime object to process</param>
        /// <returns></returns>
        public static object AsValueOrDbNull(this DateTime? target)
        {
            if (!target.HasValue)
                return DBNull.Value;

            return target.Value;
        }

    }
}
