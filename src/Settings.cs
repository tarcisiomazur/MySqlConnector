using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Reflection;

namespace MySqlConnector
{
    internal class Settings
    {
        public Settings(string cfgPath)
        {
            Load(cfgPath);
        }

        internal string Server { get; set; }
        internal string Database { get; set; }
        internal ushort Port { get; set; }
        internal string UserID { get; set; }
        internal string Password { get; set; }
        internal bool ForwardEngineer { get; set; }
        internal bool SkipVerification { get; set; }

        private void Load(string cfgPath)
        {
            foreach (var rawLine in File.ReadAllLines(cfgPath))
            {
                var line = rawLine.Trim();

                if (line.StartsWith("#"))
                    continue;

                var split = line.Split(new[] {'='}, 2);
                if (split.Length != 2)
                    continue;

                var currentKey = split[0].Trim();
                var currentValue = split[1].Trim();
                typeof(Settings).GetProperties(BindingFlags.Instance | BindingFlags.NonPublic)
                    .Where(info => info.Name.Equals(currentKey)).Do(info =>
                    {
                        var converter = TypeDescriptor.GetConverter(info.PropertyType);
                        info.SetValue(this, converter.ConvertFromString(currentValue));
                    });
            }
        }
    }
}