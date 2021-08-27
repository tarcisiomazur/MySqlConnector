using System;
using System.ComponentModel;
using System.IO;
using System.Linq;
using Persistence;

namespace MySqlConnector.MySqlConnector
{
    public static class Settings
    {
        static Settings()
        {
            //Load($"{Directory.GetCurrentDirectory()}\\Database.cfg");
            Load("C:\\Users\\tarci\\OneDrive\\Projeto de Software\\Loje\\Loje\\Database.cfg");
        }
        
        public static string Server { get; set; }
        public static string Database { get; set; }
        public static ushort Port { get; set; }
        public static string UserID { get; set; }
        public static string Password { get; set; }
        
        public static void Load(string filename)
        {
            foreach (var rawLine in File.ReadAllLines(filename))
            {
                var line = rawLine.Trim();

                if (line.StartsWith("#"))
                    continue;

                var split = line.Split(new[] { '=' }, 2);
                if (split.Length != 2)
                    continue;

                var currentKey = split[0].Trim();
                var currentValue = split[1].Trim();
                typeof(Settings).GetProperties().Where(info => info.Name.Equals(currentKey)).Do(info =>
                {
                    var converter = TypeDescriptor.GetConverter(info.PropertyType);
                    info.SetValue(null, converter.ConvertFromString(currentValue));
                });
            }
        }
    }
}