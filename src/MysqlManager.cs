using System;
using System.Data;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace MySqlConnector
{
    internal class MysqlManager
    {
        private MySqlConnection _connection;
        internal Settings Settings;
        private string MySqlString;
        internal event EventHandler Connected;
        internal event EventHandler Disconnected;

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
                Logging = true,
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
            _connection.StateChange += ChangedState;
            
            _connection?.Open();
        }

        private void ChangedState(object sender, StateChangeEventArgs e)
        {
            Console.WriteLine(e.OriginalState + " " + e.CurrentState);
            if (e.CurrentState == e.OriginalState) return;
            switch (e.CurrentState)
            {
                case ConnectionState.Open:
                    Connected?.Invoke(sender, EventArgs.Empty);
                    break;
                case ConnectionState.Closed:
                    Disconnected?.Invoke(sender, EventArgs.Empty);
                    break;
            }
        }

        public bool Execute(string query)
        {
            try
            {
                var cmd = GetConn().CreateCommand();
                cmd.CommandText = query;
                cmd.ExecuteNonQuery();
            }
            catch (MySqlException ex)
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
            catch (Exception ex)
            {
                Console.WriteLine(ex);
                return 0;
            }

            return 1;
        }

        public MySqlCommand CreateCommand()
        {
            return GetConn().CreateCommand();
        }
        
        public MySqlCommand CreateCommand(ref IDbTransaction dbTransaction)
        {
            var command = GetConn().CreateCommand();
            if (dbTransaction is not MySqlTransaction or null)
            {
                var task = GetConn().BeginTransactionAsync();
                if (Task.WhenAny(task, Task.Delay(10000)).Result == task) {
                    Console.WriteLine("terminei");
                    dbTransaction = task.Result;
                } else {
                    Console.WriteLine("time out");
                }
            }
            command.Transaction = (MySqlTransaction) dbTransaction;
            return command;
        }
        
        public MySqlCommand GetCommand()
        {
            var command = GetConn().CreateCommand();
            command.Transaction = GetConn().BeginTransaction();
            return command;
        }
    }
}