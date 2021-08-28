using System;
using System.Data;
using MySql.Data.MySqlClient;

namespace MySqlConnector
{
    public static class MysqlManager
    {
        private static MySqlClientFactory _factory;
        private static MySqlConnection _connection;
        private static string MySqlString;

        public static void Init()
        {
            MySqlString = new MySqlConnectionStringBuilder
            {
                Server = Settings.Server,
                Database = Settings.Database,
                Port = Settings.Port,
                UserID = Settings.UserID,
                Password = Settings.Password,
                SslMode = MySqlSslMode.Required,
                AllowUserVariables = true,
            }.ConnectionString;
        }
        public static MySqlConnection GetConn()
        {
            if (_connection != null) return _connection;
            if (MySqlString == null)
            {
                //MessageBox.Show("MySqlManager não inicializado");
                Environment.Exit(5);
            }
            try
            {
                Connect();
            }
            catch (Exception ex)
            {
                //MessageBox.Show($"Erro ao se conectar com {MySqlString}: {ex}");
                Environment.Exit(2);
            }
            return _connection;
        }

        private static void Connect()
        {
            _connection = new MySqlConnection(MySqlString);
            _connection?.Open();
        }
        
        public static bool Execute(string query)
        {
            try
            {
                var cmd = GetConn().CreateCommand();
                cmd.CommandText = query;
                cmd.ExecuteNonQuery();
            }
            catch(MySqlException ex)
            {
                Console.WriteLine(ex);
                return false;
            }

            return true;
        }


        public static int Result(string query)
        {
            try
            {
                var cmd = GetConn().CreateCommand();
                cmd.CommandText = query;
                MySqlDataAdapter data = new MySqlDataAdapter(cmd);
                DataTable dt = new DataTable();
                data.Fill(dt);
            }
            catch(Exception ex)
            {
                Console.WriteLine(ex);
                return 0;
            }

            return 1;
        }

        public static MySqlCommand CreateTransaction()
        {
            var command = GetConn().CreateCommand();
            command.Transaction = GetConn().BeginTransaction();
            return command;
        }
    }
}