using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using MySql.Data.MySqlClient;
using Persistence;

namespace MySqlConnector
{
    public class MySqlProtocol : ISQL
    {
        private readonly DbExecutor _executor;
        internal readonly MysqlManager _mysqlManager;
        private ISQL _isqlImplementation;
        public string DefaultSchema => _mysqlManager.Settings.Database;
        public bool ForwardEngineer
        {
            get => _mysqlManager.Settings.ForwardEngineer;
            set => _mysqlManager.Settings.ForwardEngineer = value;
        }
        public bool SkipVerification
        {
            get => _mysqlManager.Settings.SkipVerification;
            set => _mysqlManager.Settings.SkipVerification = value;
        }

        public enum Key
        {
            PRI,
            UNI,
            MUL,
            Null,
        }

        public MySqlProtocol(string cfgPath)
        {
            _mysqlManager = new MysqlManager();
            _executor = new DbExecutor(_mysqlManager);
            _mysqlManager.Settings = new Settings(cfgPath);
            _mysqlManager.Init();
            ShowTriggers();
        }

        private void ShowTables()
        {
            var ret = new List<string>();
            var cmd = _mysqlManager.GetConn().CreateCommand();
            cmd.CommandText = "show tables from sismaper";
            var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                ret.Add(reader.GetString(0));
            }
            reader.Close();
            
            foreach (var s in ret)
            {
                cmd.CommandText = "show create table sismaper." + s;
                try
                {
                    reader = cmd.ExecuteReader();
                    reader.Read();
                    var str = reader.GetString(1);
                    str = str.Remove(str.IndexOf("ENGINE=", StringComparison.CurrentCultureIgnoreCase));
                    Console.WriteLine(str + ";\n");
                    reader.Close();
                }
                catch
                {
                    
                }
            }

        }
        private void ShowTriggers()
        {
            var cmd = _mysqlManager.GetConn().CreateCommand();
            cmd.CommandText = "show triggers from sismaper";
            var reader = cmd.ExecuteReader();
            Console.WriteLine("DELIMITER $$");
            while (reader.Read())
            {
                var trg = reader.GetString("Trigger");
                if(trg.Contains("Version", StringComparison.CurrentCultureIgnoreCase))
                    continue;
                var stmt = reader.GetString("Statement");
                var ev = reader.GetString("Event");
                var ti = reader.GetString("Timing");
                var tb = reader.GetString("Table");
                Console.WriteLine($"CREATE TRIGGER `{trg}` {ti} {ev} ON `{tb}` FOR EACH ROW\n{stmt}$$\n");
            }
            Console.WriteLine("DELIMITER ;");
            reader.Close();

        }

        bool ISQL.ExistTable(Table t)
        {
            var cmd = _mysqlManager.GetConn().CreateCommand();
            cmd.CommandText =
                $"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=\"{t.Schema}\" AND TABLE_NAME=\"{t.SqlName}\"";
            using var reader = cmd.ExecuteReader();
            return reader.HasRows;
        }

        DbDataReader ISQL.LoadTable(Table table)
        {
            var cmd = _mysqlManager.GetConn().CreateCommand();
            cmd.CommandText = $"SHOW COLUMNS FROM {table.Schema}.{table.SqlName}";
            try
            {
                var reader = cmd.ExecuteReader();
                return reader;
            }
            catch (Exception ex)
            {
                //if (!ex.ToString().Contains("Error Code: 1146"))
                throw new MySqlConnectorException($"Error on load Table {table.SqlName}", ex);
            }
        }

        bool ISQL.ValidateField(Table table, Field field)
        {
            if (SkipVerification) return true;
            var tableCols = _executor.LoadTableColumns(table.SqlName, table.Schema);
            var sch = tableCols.FirstOrDefault(schema =>
                schema.COLUMN_NAME.Equals(field.SqlName));
            if (sch != default &&
                sch.COLUMN_TYPE.Equals(GetSqlFieldType(field), StringComparison.InvariantCultureIgnoreCase))
            {
                return true;
            }

            return ForwardEngineer && _executor.UpdateField(table, field, tableCols);
        }


        bool ISQL.ValidadeForeignKeys(Table table, Relationship relationship)
        {
            if (SkipVerification) return true;
            var tableCols = _executor.LoadTableColumns(table.SqlName, table.Schema);
            var keys = _executor.LoadTableKeys(table.SqlName, table.Schema);

            foreach (var (name, field) in relationship.Links)
            {
                if (!tableCols.Any(cts => cts.COLUMN_TYPE.SQLEquals(GetSqlFieldType(field)) &&
                                          cts.COLUMN_NAME.SQLEquals(name)) ||
                    !keys.Any(key => key.CONSTRAINT_NAME.SQLEquals(relationship.FkName) &&
                                     key.COLUMN_TYPE.SQLEquals(GetSqlFieldType(field)) &&
                                     key.COLUMN_NAME.SQLEquals(name)))
                {
                    return ForwardEngineer && _executor.UpdateForeignKeys(table, relationship, tableCols, keys);
                }
            }

            return true;
        }

        bool ISQL.ValidatePrimaryKeys(Table table, List<PrimaryKey> primaryKeys)
        {
            if (SkipVerification) return true;
            var tableCols = _executor.LoadTableColumns(table.SqlName, table.Schema);
            var keys = _executor.LoadTableKeys(table.SqlName, table.Schema)
                .Where(key => key.CONSTRAINT_NAME == "PRIMARY").ToList();
            foreach (var pk in primaryKeys)
            {
                if (!tableCols.Any(cts => cts.COLUMN_TYPE.SQLEquals(GetSqlFieldType(pk)) &&
                                          cts.COLUMN_NAME.SQLEquals(pk.SqlName)) ||
                    !keys.Any(key => key.COLUMN_TYPE.SQLEquals(GetSqlFieldType(pk)) &&
                                     key.COLUMN_NAME.SQLEquals(pk.SqlName)))
                {
                    return ForwardEngineer && _executor.UpdatePrimaryKeys(table, primaryKeys, tableCols, keys);
                }
            }

            return true;
        }

        long ISQL.Update(Table table, Dictionary<string, object> fields, Dictionary<PropColumn, object> keys)
        {
            var command =  "UPDATE @schema.@table SET @fieldAndValue WHERE(@keyAndValue)";
            
            var cmd = _mysqlManager.GetConn().CreateCommand();
            cmd.CommandText = command;
            cmd.SetParameter("@schema", table.Schema);
            cmd.SetParameter("@table", table.SqlName);
            cmd.SetParameter("@fieldAndValue", fields.Join(",", pair => $"{pair.Key} = {_ConvertValueToString(pair.Value)}"));
            cmd.SetParameter("@keyAndValue", keys.Join(" AND ", pair => $"{pair.Key.SqlName} = {_ConvertValueToString(pair.Value)}"));
            
            try
            {
                var exec = cmd.ExecuteNonQuery();
                return exec == 1 && table.DefaultPk ? cmd.LastInsertedId : exec;
            }
            catch (MySqlException ex)
            {
                throw new MySqlConnectorException($"MySql Error on save {table.SqlName}", ex);
            }
        }

        long ISQL.Insert(Table table, Dictionary<string, object> fields)
        {
            var command = "INSERT INTO @schema.@table (@onlyFields) VALUES(@onlyValues) "+
                          "ON DUPLICATE KEY UPDATE @fieldEqualsValue";
            var cmd = _mysqlManager.GetConn().CreateCommand();
            cmd.CommandText = command;
            cmd.SetParameter("@schema", table.Schema);
            cmd.SetParameter("@table", table.SqlName);
            cmd.SetParameter("@onlyFields", string.Join(",", fields.Keys));
            cmd.SetParameter("@onlyValues", fields.Join(",", pair => _ConvertValueToString(pair.Value)));
            cmd.SetParameter("@fieldEqualsValue", fields.Join(",", pair => $"{pair.Key}={_ConvertValueToString(pair.Value)}"));

            //Console.WriteLine(cmd.CommandText);
            try
            {
                var exec = cmd.ExecuteNonQuery();
                return exec == 1 && table.DefaultPk ? cmd.LastInsertedId : exec;
            }
            catch (MySqlException ex)
            {
                throw new MySqlConnectorException($"MySql Error on save {table.SqlName}", ex);
            }
        }

        DbDataReader ISQL.Select(Table table, Dictionary<string, object> keys, uint offset, uint length)
        {
            var cmd = _mysqlManager.GetConn().CreateCommand();
            var command =
                $"SELECT *FROM {table.SqlName} " +
                $"WHERE {string.Join(" AND ", keys.Select(pair => $"{pair.Key} = {_ConvertValueToString(pair.Value)}"))} "+
                $"LIMIT {offset},{length}";
            cmd.CommandText = command;
            try
            {
                var reader = cmd.ExecuteReader();
                return reader;
            }
            catch (Exception ex)
            {
                throw new MySqlConnectorException("ExecuteReader returned error on Select", ex);
            }
        }

        DbDataReader ISQL.SelectWhereQuery(Table table, string query, uint offset, uint length)
        {
            var cmd = _mysqlManager.GetConn().CreateCommand();
            var command = $"SELECT *FROM @schema.@table WHERE @query LIMIT {offset},{length}";
            cmd.CommandText = command;
            cmd.SetParameter("@schema", table.Schema);
            cmd.SetParameter("@table", table.SqlName);
            cmd.SetParameter("@query", query);
            try
            {
                var reader = cmd.ExecuteReader();
                return reader;
            }
            catch (Exception ex)
            {
                throw new MySqlConnectorException($"ExecuteReader returned error on Select Query ({cmd.CommandText})", ex);
            }
        }

        DbDataReader ISQL.ExecuteProcedure(string procedureName, Dictionary<string,object> parameters)
        {
            var cmd = _mysqlManager.GetConn().CreateCommand();
            cmd.CommandType = CommandType.StoredProcedure;
            cmd.CommandText = procedureName;
            
            foreach (var (key,value) in parameters)
            {
                cmd.Parameters.Add(new MySqlParameter(key,value));
            }

            try
            {
                cmd.Prepare();
                var reader = cmd.ExecuteReader();
                return reader;
            }
            catch(Exception ex)
            {
                throw new MySqlConnectorException($"ExecuteReader returned error on Call Procedure {procedureName}", ex);
            }
        }

        DbDataReader ISQL.SelectView(string name, string schema)
        {
            var cmd = _mysqlManager.GetConn().CreateCommand();
            cmd.CommandText = "SELECT * FROM @schema.@table";
            cmd.SetParameter("@schema", schema);
            cmd.SetParameter("@table", name);
            
            try
            {
                cmd.Prepare();
                var reader = cmd.ExecuteReader();
                return reader;
            }
            catch(Exception ex)
            {
                throw new MySqlConnectorException($"ExecuteReader returned error on SelectView {schema}.{name}", ex);
            }
        }

        bool ISQL.Delete(Table table, Dictionary<string, object> keys)
        {
            const string command = "DELETE FROM @schema.@table WHERE @where";

            var cmd = _mysqlManager.CreateTransaction();
            cmd.CommandText = command;
            cmd.SetParameter("@schema", table.Schema);
            cmd.SetParameter("@table", table.SqlName);
            cmd.SetParameter("@where", keys.Join(" AND ", pair => $"{pair.Key} = {_ConvertValueToString(pair.Value)}"));
            try
            {
                var rows = cmd.ExecuteNonQuery();
                if (rows > 1)
                    cmd.Transaction.Rollback();
                else
                    cmd.Transaction.Commit();
                return rows == 1;
            }
            catch (Exception ex)
            {
                throw new MySqlConnectorException("ExecuteReader returned error on Delete", ex);
            }
        }

        uint ISQL.SelectCount(Table table, Dictionary<string, object> keys)
        {
            var cmd = _mysqlManager.GetConn().CreateCommand();
            var command = $"SELECT COUNT(*) {table.SqlName}" +
                          $"WHERE {string.Join(" AND ", keys.Select(pair => $"{pair.Key} = {_ConvertValueToString(pair.Value)}"))}";
            cmd.CommandText = command;
            var reader = cmd.ExecuteReader();
            if (reader.Read())
            {
                return reader.GetUInt32(0);
            }

            return 0;
        }        
        
        uint ISQL.SelectCountWhereQuery(Table table, string likeQuery)
        {
            var cmd = _mysqlManager.GetConn().CreateCommand();
            var command = $"SELECT COUNT(*) {table.SqlName} WHERE {likeQuery}";
            cmd.CommandText = command;
            var reader = cmd.ExecuteReader();
            if (reader.Read())
            {
                return reader.GetUInt32(0);
            }

            return 0;
        }

        KeyType ISQL.GetKeyType(string key)
        {
            return key switch
            {
                "MUL" => KeyType.ForeignKey,
                "PRI" => KeyType.PrimaryKey,
                _ => KeyType.Null
            };
        }

        bool ISQL.ExistTrigger(Table table, string triggerName)
        {
            var query = $"SELECT * FROM INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_SCHEMA='{table.Schema}' " +
                        $"AND EVENT_OBJECT_TABLE='{table.SqlName}' AND TRIGGER_NAME = '{triggerName}'";
            return _executor.HasRows(query);
        }

        void ISQL.CreateTrigger(Table table, string sqlTrigger, string triggerName, ISQL.SqlTriggerType sqlTriggerType)
        {
            if (ForwardEngineer == false) return;
            var query = $"\nCREATE TRIGGER {triggerName} {sqlTriggerType.ToString().Replace("_", " ")} ON {table.SqlName} FOR EACH ROW BEGIN " +
                                sqlTrigger + "//";
            _executor.ExecuteScript(query);
        }

        string ISQL.ConvertValueToString(object value) => _ConvertValueToString(value);

        internal static string GetSqlFieldType(Field field)
        {
            switch (field.SqlType)
            {
                case SqlDbType.VarChar:
                case SqlDbType.Binary:
                case SqlDbType.Bit:
                    return $"{field.SqlType.ToString()}({field.Length})";
                case SqlDbType.Decimal:
                    return $"{field.SqlType.ToString()}({field.Length},{field.Precision})";
                case SqlDbType.Float:
                    return $"FLOAT({field.Length},{field.Precision})";
                case SqlDbType.Real:
                    return $"DOUBLE({field.Length},{field.Precision})";
                case SqlDbType.Int:
                case SqlDbType.BigInt:
                    return field.SqlType.ToString().ToUpper();
                case SqlDbType.Xml:
                    return "BLOB";
                default:
                    return field.SqlType.ToString();
            }
        }

        internal static string _ConvertValueToString(object value)
        {
            if (value == null) return "NULL";
            return value switch
            {
                decimal @decimal => @decimal.ToString(CultureInfo.InvariantCulture),
                string @string => $"'{@string.Replace("'", "\\'").Replace(";", "\\;")}'",
                DateTime dateTime => $"{dateTime.ToSql()}",
                _ => value.ToString()
            };
        }





    }

}