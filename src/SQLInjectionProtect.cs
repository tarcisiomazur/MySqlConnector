using System;
using System.Collections.Generic;
using System.Linq;

namespace MySqlConnector
{
    public class SQLInjectionProtect
    {
        private static List<string> _restrictKeyWords;

        static SQLInjectionProtect()
        {
            _restrictKeyWords = new List<string>();
            _restrictKeyWords.Add("SELECT");
            _restrictKeyWords.Add("WHERE");
            _restrictKeyWords.Add("FROM");
            _restrictKeyWords.Add("JOIN");
            _restrictKeyWords.Add("ON");
        }
        public static void CheckParameter(string str)
        {
            
        }
    }
}