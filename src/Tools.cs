using System;
using System.Collections.Generic;
using Nullable = Persistence.Nullable;

namespace MySqlConnector
{
    public static class Tools
    {
        public static string ToSql(this Nullable nullable)
        {
            return nullable == Nullable.Null ? "NULL" : "NOT NULL";
        }
        public static bool SQLEquals(this string s1, string s2)
        {
            return s1.Equals(s2, StringComparison.CurrentCultureIgnoreCase);
        }

        public static string Join<TSource>(this IEnumerable<TSource> source, string separator,
            Func<TSource, string> func)
        {
            var enumerator = source.GetEnumerator();
            if (!enumerator.MoveNext()) return "";
            var result = func(enumerator.Current);
            while (enumerator.MoveNext())
            {
                result += separator + func(enumerator.Current);
            }

            return result;
        }

    }
}