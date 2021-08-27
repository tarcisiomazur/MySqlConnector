using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Reflection.Metadata;
using MySql.Data.MySqlClient;
using Persistence;

namespace MySqlConnector.MySqlConnector
{
    public class MySqlProtocol : ISQL
    {
        public string DefaultSchema => Settings.Database;

        public bool ForwardEngineer { get; set; }
        public bool SkipVerification { get; set; }

        public enum Key
        {
            PRI,
            UNI,
            MUL,
            Null,
        }

        public MySqlProtocol()
        {
            MysqlManager.Init();
        }

        bool ISQL.ExistTable(Table t)
        {
            var cmd = MysqlManager.GetConn().CreateCommand();
            cmd.CommandText =
                $"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=\"{t.Schema}\" AND TABLE_NAME=\"{t.SqlName}\"";
            using var reader = cmd.ExecuteReader();
            return reader.HasRows;
        }

        DbDataReader ISQL.LoadTable(Table table)
        {
            var cmd = MysqlManager.GetConn().CreateCommand();
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
            var tableCols = DbExecutor.LoadTableColumns(table.SqlName, table.Schema);
            var sch = tableCols.FirstOrDefault(schema =>
                schema.COLUMN_NAME.Equals(field.SqlName));
            if (sch != default &&
                sch.COLUMN_TYPE.Equals(GetSqlFieldType(field), StringComparison.InvariantCultureIgnoreCase))
            {
                return true;
            }

            if (ForwardEngineer)
            {
                return DbExecutor.UpdateField(table, field, tableCols);
            }

            return false;
        }


        bool ISQL.ValidadeForeignKeys(Table table, ManyToOne manyToOne)
        {
            if (SkipVerification) return true;
            var tableCols = DbExecutor.LoadTableColumns(table.SqlName, table.Schema);
            var keys = DbExecutor.LoadTableKeys(table.SqlName, table.Schema);
            
            foreach (var field in manyToOne.Links)
            {
                if (!tableCols.Any(cts => cts.COLUMN_TYPE.SQLEquals(GetSqlFieldType(field)) && 
                                          cts.COLUMN_NAME.SQLEquals(field.SqlName)) ||
                    !keys.Any(key => key.CONSTRAINT_NAME.SQLEquals(manyToOne.FkName) && 
                                     key.COLUMN_TYPE.SQLEquals(GetSqlFieldType(field)) &&
                                     key.COLUMN_NAME.SQLEquals(manyToOne.FieldFkName(field))))
                {
                    return ForwardEngineer && DbExecutor.UpdateForeignKeys(table, manyToOne, tableCols, keys);
                }
            }
            return false;
        }

        bool ISQL.ValidatePrimaryKeys(Table table, List<PrimaryKey> primaryKeys)
        {
            if (SkipVerification) return true;
            var tableCols = DbExecutor.LoadTableColumns(table.SqlName, table.Schema);
            var keys = DbExecutor.LoadTableKeys(table.SqlName, table.Schema);
            foreach (var pk in primaryKeys)
            {
                if (!tableCols.Any(cts => cts.COLUMN_TYPE.SQLEquals(GetSqlFieldType(pk)) && 
                                          cts.COLUMN_NAME.SQLEquals(pk.SqlName)) ||
                    !keys.Any(key => key.CONSTRAINT_NAME == "PRIMARY" &&
                                     key.COLUMN_TYPE.SQLEquals(GetSqlFieldType(pk)) &&
                                     key.COLUMN_NAME.SQLEquals(pk.SqlName)))
                {
                    return ForwardEngineer && DbExecutor.UpdatePrimaryKeys(table, primaryKeys, tableCols, keys);
                }
            }

            return true;
        }

        long ISQL.InsertOrUpdate(Table table, Dictionary<string, object> fields)
        {
            var command = $"INSERT INTO {table.SqlName} ({string.Join(",", fields.Keys)}) " +
                          $"VALUES({string.Join(",", fields.Values.Select(_ConvertValueToString))}) " +
                          $"ON DUPLICATE KEY UPDATE {string.Join(", ", fields.Select(pair => $"{pair.Key} = {_ConvertValueToString(pair.Value)}"))}";

            var cmd = MysqlManager.GetConn().CreateCommand();
            Console.WriteLine(command);
            cmd.CommandText = command;
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

        DbDataReader ISQL.Select(Table table, Dictionary<string, object> keys)
        {
            var cmd = MysqlManager.GetConn().CreateCommand();
            var command =
                $"SELECT *FROM {table.SqlName} " +
                $"WHERE {string.Join(", ", keys.Select(pair => $"{pair.Key} = {_ConvertValueToString(pair.Value)}"))}";
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

        DbDataReader ISQL.Select(Table table, Dictionary<string, object> keys, long first, long count)
        {
            var cmd = MysqlManager.GetConn().CreateCommand();
            var command =
                $"SELECT *FROM {table.SqlName} " +
                $"WHERE {string.Join(", ", keys.Select(pair => $"{pair.Key} = {_ConvertValueToString(pair.Value)}"))}" +
                $"LIMITED({first},{count})";
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

        bool ISQL.Delete(Table table, Dictionary<string, object> keys)
        {
            var command = $"DELETE FROM {table.SqlName} " +
                          $"WHERE {string.Join(", ", keys.Select(pair => $"{pair.Key} = {_ConvertValueToString(pair.Value)}"))}";
            try
            {
                var cmd = MysqlManager.GetConn().CreateCommand();
                cmd.CommandText = command;
                return cmd.ExecuteNonQuery() == 1;
            }
            catch (Exception ex)
            {
                throw new MySqlConnectorException("ExecuteReader returned error on Delete", ex);
            }
        }

        long ISQL.SelectCount(Table table, Dictionary<string, object> keys)
        {
            var cmd = MysqlManager.GetConn().CreateCommand();
            var command = $"SELECT count(*) {table.SqlName}" +
                          $"WHERE {string.Join(", ", keys.Select(pair => $"{pair.Key} = {_ConvertValueToString(pair.Value)}"))}";
            cmd.CommandText = command;
            var reader = cmd.ExecuteReader();
            if (reader.Read())
            {
                return reader.GetInt64(0);
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
            return DbExecutor.HasRows(query);
        }

        void ISQL.CreateTrigger(Table table, string sqlTrigger, string triggerName, ISQL.SqlTriggerType sqlTriggerType)
        {
            var query = $"\nCREATE TRIGGER {triggerName} {sqlTriggerType.ToString().Replace("_", " ")} ON {table.SqlName} FOR EACH ROW BEGIN " +
                                sqlTrigger + "//";
            DbExecutor.ExecuteScript(query);
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
            return value switch
            {
                decimal @decimal => @decimal.ToString(CultureInfo.InvariantCulture),
                string @string => $"'{@string}'",
                _ => value?.ToString()
            };
        }

        



    }

}