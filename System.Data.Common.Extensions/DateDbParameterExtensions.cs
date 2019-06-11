using System;
using System.Collections.Generic;
using System.Text;

namespace System
{
    public static class DateDbParameterExtensions
    {
        public static object AsValueOrDbNull(this DateTime? target)
        {
            if (!target.HasValue)
                return DBNull.Value;

            return target;
        }

    }
}
