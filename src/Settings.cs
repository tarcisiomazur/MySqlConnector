using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;

namespace MySqlConnector
{
    internal static class Settings
    {
        static Settings()
        {
            //Load($"{Directory.GetCurrentDirectory()}\\Database.cfg");
            Load("C:\\Users\\tarci\\OneDrive\\Projeto de Software\\Loje\\Loje\\Database.cfg");
        }
        
        internal static string Server { get; set; }
        internal static string Database { get; set; }
        internal static ushort Port { get; set; }
        internal static string UserID { get; set; }
        internal static string Password { get; set; }

        private static void Load(string filename)
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
                typeof(Settings).GetProperties(BindingFlags.Static | BindingFlags.NonPublic).Where(info => info.Name.Equals(currentKey)).Do(info =>
                {
                    var converter = TypeDescriptor.GetConverter(info.PropertyType);
                    info.SetValue(null, converter.ConvertFromString(currentValue));
                });
            }
        }
    }
}