using System;
using System.Collections.Generic;
using System.Linq;

namespace MySqlConnector
{
    public class SQLInjectionProtect
    {
        private static List<string> restrictKeyWords;

        static SQLInjectionProtect()
        {
            restrictKeyWords = new List<string>();
            restrictKeyWords.Add("SELECT");
            restrictKeyWords.Add("WHERE");
            restrictKeyWords.Add("FROM");
            restrictKeyWords.Add("JOIN");
            restrictKeyWords.Add("ON");
        }
        public static void CheckParameter(string str)
        {
            
        }
    }
}