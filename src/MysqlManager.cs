using System;
using System.Data;
using MySql.Data.MySqlClient;

namespace MySqlConnector
{
    internal class MysqlManager
    {
        private MySqlConnection _connection;
        internal Settings Settings;
        private string MySqlString;

        public void Init()
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
        public MySqlConnection GetConn()
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
            catch (Exception)
            {
                //MessageBox.Show($"Erro ao se conectar com {MySqlString}: {ex}");
                //Environment.Exit(2);
            }
            return _connection;
        }

        private void Connect()
        {
            _connection = new MySqlConnection(MySqlString);
            _connection?.Open();
        }
        
        public bool Execute(string query)
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


        public int Result(string query)
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

        public MySqlCommand CreateTransaction()
        {
            var command = GetConn().CreateCommand();
            command.Transaction = GetConn().BeginTransaction();
            return command;
        }
    }
}