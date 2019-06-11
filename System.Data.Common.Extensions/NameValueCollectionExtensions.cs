using System;
using System.Collections.Generic;
using System.Text;

namespace System.Collections.Specialized
{
    public static class NameValueCollectionExtensions
    {
        public static int ValueAsInt(this NameValueCollection target, string key)
        {
            var aValue = target[key] as String;

            if (aValue is null)
                throw new NullReferenceException(String.Format("The key {0} has not been set in the NameValueCollection", key));

            int outInt = 0;

            if (!int.TryParse(aValue, out outInt))
                throw new InvalidCastException(string.Format("The key {0} in the NameValueCollection has a value of {1} which is not a valid int", key, aValue));


            return outInt;

        }
    }
}

