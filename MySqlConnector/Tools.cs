
using System;
using Nullable = Persistence.Nullable;

namespace MySqlConnector.MySqlConnector
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
    }
}