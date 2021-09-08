using System;
using System.Collections.Generic;
using MySql.Data.MySqlClient;
using Nullable = Persistence.Nullable;

namespace MySqlConnector
{
    internal static class Tools
    {
        public static void SetParameter(this MySqlCommand command, string parameter, object value)
        {
            command.CommandText = command.CommandText.Replace(parameter, value.ToString());
        }
        
        public static void Do<T>(this IEnumerable<T> sequence, Action<T> action)
        {
            if (sequence == null)
                return;
            IEnumerator<T> enumerator = sequence.GetEnumerator();
            while (enumerator.MoveNext())
                action(enumerator.Current);
        }
        
        public static string ToSql(this Nullable nullable)
        {
            return nullable == Nullable.Null ? "NULL" : "NOT NULL";
        }
        
        public static string ToSql(this DateTime dateTime)
        {
            return dateTime == DateTime.MinValue? "NULL" : $"'{dateTime:yyyy/MM/dd HH:mm:ss}'";
            
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