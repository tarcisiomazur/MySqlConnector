using System;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace MySqlConnector
{
    internal class MysqlManager
    {
        private MySqlConnection _connection;
        internal Settings Settings;
        private string MySqlString;
        internal event Action Connected;
        internal event Action Disconnected;
        internal event Action Reconnecting;
        internal bool IsConnected { get; set; }

        public void Init()
        {
            MySqlString = new MySqlConnectionStringBuilder
            {
                Server = Settings.Server,
                Port = Settings.Port,
                UserID = Settings.UserID,
                Password = Settings.Password,
                SslMode = MySqlSslMode.Required,
                Logging = true,
                MaximumPoolSize = Settings.MaximumPoolSize,
                AllowUserVariables = true,
            }.ConnectionString;

            if (!Settings.AutoReconnect) return;

            if (Settings.MonitorIntervalTime > 0)
            {
                TestConnection();
            }
        }

        public MySqlConnection GetConn()
        {
            if (MySqlString == null)
            {
                Environment.Exit(5);
            }

            var newConnection = new MySqlConnection(MySqlString);
            while (true)
            {
                try
                {
                    newConnection.Open();
                    if (newConnection.State is ConnectionState.Open)
                    {
                        if (!IsConnected)
                        {
                            Connected?.Invoke();
                            IsConnected = true;
                        }

                        return newConnection;
                    }
                }
                catch
                {
                    Thread.Sleep(1000);
                }

                if (IsConnected)
                    IsConnected = false;
                else
                    continue;
                Disconnected?.Invoke();
                if (Settings.AutoReconnect)
                    Reconnecting?.Invoke();
            }
        }

        private async Task OnDisconnect()
        {
            Disconnected?.Invoke();
            await Task.Delay(1000);
            if (Settings.AutoReconnect)
                Reconnecting?.Invoke();
        }

        private async Task TestConnection()
        {
            _connection = new MySqlConnection(MySqlString);
            var cmd = new MySqlCommand("SELECT 1", _connection);
            while (true)
            {
                await Task.Delay(Settings.MonitorIntervalTime);
                try
                {
                    if (_connection.State != ConnectionState.Open)
                        _connection.Open();
                    cmd.ExecuteNonQuery();
                    _connection.Close();
                    continue;
                }
                catch
                {
                    if (!Settings.AutoReconnect) continue;
                }

                try
                {
                    await _connection.OpenAsync();
                }
                catch
                {
                }

                if (_connection.State != ConnectionState.Open && IsConnected)
                {
                    Disconnected?.Invoke();
                    await Task.Delay(1000);
                    Reconnecting?.Invoke();
                }

                while (_connection.State != ConnectionState.Open)
                {
                    try
                    {
                        _connection.Open();
                    }
                    catch
                    {
                        await Task.Delay(5000);
                    }
                }

                if (_connection.State == ConnectionState.Open)
                {
                    Connected?.Invoke();
                }
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
            if (dbTransaction is MySqlTransaction sqlDbTransaction)
            {
                return sqlDbTransaction.Connection.CreateCommand();
            }

            var conn = GetConn();
            var command = conn.CreateCommand();
            var task = conn.BeginTransactionAsync();
            if (Task.WhenAny(task, Task.Delay(10000)).Result == task)
            {
                dbTransaction = task.Result;
            }
            else
            {
                Console.WriteLine("time out");
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