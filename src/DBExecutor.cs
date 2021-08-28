using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using MySql.Data.MySqlClient;
using Persistence;
using Nullable = Persistence.Nullable;

namespace MySqlConnector
{
    public static class DbExecutor
    {
        public static void SetParameter(this MySqlCommand command, string parameter, object value)
        {
            command.CommandText = command.CommandText.Replace(parameter, value.ToString());
        }

        private static bool IndexExist(string table, string schema, string index)
        {
            var cmd = MysqlManager.GetConn().CreateCommand();
            cmd.CommandText =
               $"SELECT * FROM INFORMATION_SCHEMA.STATISTICS  WHERE TABLE_NAME = '{table}' AND INDEX_NAME = '{index}' AND INDEX_SCHEMA='{schema}'";
            MySqlDataReader reader = null;
            try
            {
                reader = cmd.ExecuteReader();
                return reader.HasRows;
            }
            catch (Exception ex)
            {
                throw new MySqlConnectorException("ExecuteReader returned error on Select Index", ex);
            }
            finally
            {
                reader?.Close();
            }
        }
        public static List<KEY_TABLE_SCHEMA> LoadTableKeys(string table, string schema)
        {
            var ret = new List<KEY_TABLE_SCHEMA>();
            var cmd = MysqlManager.GetConn().CreateCommand();
            var command =
                "SELECT K.TABLE_NAME, K.COLUMN_NAME, COL.DATA_TYPE, COL.COLUMN_TYPE, K.CONSTRAINT_NAME, REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME " +
                "FROM INFORMATION_SCHEMA.COLUMNS COL JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE K " +
                "ON COL.TABLE_SCHEMA = K.TABLE_SCHEMA AND COL.TABLE_NAME = K.TABLE_NAME AND COL.COLUMN_NAME = K.COLUMN_NAME " +
                $"WHERE COL.TABLE_SCHEMA = \"{schema}\" AND K.TABLE_NAME = \"{table}\"";

            cmd.CommandText = command;
            MySqlDataReader reader = null;
            try
            {
                reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    var ts = new KEY_TABLE_SCHEMA();
                    foreach (var pi in typeof(KEY_TABLE_SCHEMA).GetProperties())
                    {
                        if (!reader.IsDBNull(pi.Name))
                        {
                            pi.SetValue(ts, reader[pi.Name] ?? "");
                        }
                    }

                    ret.Add(ts);
                }

                reader.Close();
            }
            catch (Exception ex)
            {
                throw new MySqlConnectorException("ExecuteReader returned error on Load Table", ex);
            }
            finally
            {
                reader?.Close();
            }

            return ret;
        }

        public static List<COLUMN_TABLE_SCHEMA> LoadTableColumns(string table, string schema)
        {
            var ret = new List<COLUMN_TABLE_SCHEMA>();
            var cmd = MysqlManager.GetConn().CreateCommand();
            var command =
                "SELECT COLUMN_NAME, COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS " +
                $"WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'";
            cmd.CommandText = command;
            MySqlDataReader reader = null;
            try
            {
                reader = cmd.ExecuteReader();
                while (reader.Read())
                {
                    var ts = new COLUMN_TABLE_SCHEMA();
                    foreach (var pi in typeof(COLUMN_TABLE_SCHEMA).GetProperties())
                    {
                        pi.SetValue(ts, reader[pi.Name] ?? "");
                    }

                    ret.Add(ts);
                }

                reader.Close();
            }
            catch (Exception ex)
            {
                throw new MySqlConnectorException("ExecuteReader returned error on Load Table", ex);
            }
            finally
            {
                reader?.Close();
            }

            return ret;
        }

        public static bool UpdateField(Table table, Field field, List<COLUMN_TABLE_SCHEMA> tableCols)
        {
            string query;
            if (tableCols.Count == 0)
                query = "CREATE TABLE @schema.@table (@field)";
            else if (tableCols.Exists(schema => schema.COLUMN_NAME == field.SqlName))
                query = "ALTER TABLE @schema.@table MODIFY @field";
            else
                query = "ALTER TABLE @schema.@table ADD COLUMN @field";

            var cmd = MysqlManager.CreateTransaction();
            cmd.CommandText = query;
            cmd.SetParameter("@schema", table.Schema);
            cmd.SetParameter("@table", table.SqlName);
            cmd.SetParameter("@field", BuildField(field));
            try
            {
                cmd.ExecuteNonQuery();
                cmd.Transaction.Commit();
            }
            catch (Exception ex)
            {
                cmd.Transaction.Rollback();
                throw new MySqlConnectorException($"Error on create table {table.SqlName} in database {table.Schema}",
                    ex);
            }

            return true;
        }

        public static bool UpdatePrimaryKeys(Table table, List<PrimaryKey> primaryKeys,
            List<COLUMN_TABLE_SCHEMA> tableCols, List<KEY_TABLE_SCHEMA> keyTableSchemata)
        {
            string query;
            if (tableCols.Count == 0)
            {
                query = "CREATE TABLE @schema.@table (@keysAndType, PRIMARY KEY (@onlykeys))";
            }
            else
            {
                var fieldChanges = "";
                foreach (var pk in primaryKeys)
                {
                    var fName = BuildPkField(pk);
                    var sch = tableCols.FirstOrDefault(_cts => _cts.COLUMN_NAME.SQLEquals(pk.SqlName));
                    if (sch == default)
                    {
                        fieldChanges += "ADD COLUMN " + fName;
                    }
                    else if (!sch.COLUMN_TYPE.SQLEquals(MySqlProtocol.GetSqlFieldType(pk)))
                    {
                        fieldChanges += "MODIFY COLUMN " + fName;
                    }
                }
                query = "ALTER TABLE @schema.@table " + fieldChanges + (fieldChanges==""?"":",");
                query += " DROP PRIMARY KEY, ADD PRIMARY KEY (@onlykeys)";
            }

            var cmd = MysqlManager.CreateTransaction();
            cmd.CommandText = query;
            cmd.SetParameter("@schema", table.Schema);
            cmd.SetParameter("@table", table.SqlName);
            cmd.SetParameter("@onlykeys", string.Join(',', primaryKeys.Select(key => key.SqlName)));
            cmd.SetParameter("@keysAndType", string.Join(",", primaryKeys.Select(BuildPkField)));
            try
            {
                Console.WriteLine(cmd.CommandText);
                cmd.ExecuteNonQuery();
                cmd.Transaction.Commit();
            }
            catch (Exception ex)
            {
                cmd.Transaction.Rollback();
                throw new MySqlConnectorException($"Error on create table {table.SqlName} in database {table.Schema}",
                    ex);
            }

            return true;
        }

        private static object BuildField(Field field)
        {
            var str = $"{field.SqlName} {MySqlProtocol.GetSqlFieldType(field)} ";
            if (field.DefaultValue != null)
                str += "DEFAULT " + MySqlProtocol._ConvertValueToString(field.DefaultValue);
            return str;
        }

        private static string BuildPkField(PrimaryKey key)
        {
            var str = $"{key.SqlName} {MySqlProtocol.GetSqlFieldType(key)} ";
            str += key.Nullable.ToSql();
            if (key.AutoIncrement)
                str += " AUTO_INCREMENT";
            else if (key.DefaultValue != null)
                str += " DEFAULT " + MySqlProtocol._ConvertValueToString(key.DefaultValue);
            return str;
        }

        private static string BuildFkField(KeyValuePair<string, Field> link, ManyToOne manyToOne)
        {
            var str = $"{link.Key} {MySqlProtocol.GetSqlFieldType(link.Value)} ";
            str += manyToOne.Nullable.ToSql();
            if (link.Value.DefaultValue != null)
                str += " DEFAULT " + MySqlProtocol._ConvertValueToString(link.Value.DefaultValue);
            return str;
        }


        public static bool UpdateForeignKeys(Table table, ManyToOne manyToOne, List<COLUMN_TABLE_SCHEMA> tableCols,
            List<KEY_TABLE_SCHEMA> keys)
        {
            var query = "";
            if (keys.Any(key => key.CONSTRAINT_NAME.SQLEquals(manyToOne.FkName)))
                query = "ALTER TABLE @schema.@table DROP FOREIGN KEY @fk_name;\n";
            if (IndexExist(table.SqlName, table.Schema, manyToOne.FkName))
                query += "ALTER TABLE @schema.@table DROP INDEX @fk_name;\n";
            
            var fieldChanges = "";
            foreach (var link in manyToOne.Links)
            {
                var sch = tableCols.FirstOrDefault(cts => cts.COLUMN_NAME.SQLEquals(link.Key));
                fieldChanges += (sch == default? "ADD COLUMN " : "MODIFY COLUMN ") + BuildFkField(link,manyToOne) + ",";
            }

            if (tableCols.Count == 0)
                query =
                    "CREATE TABLE @schema.@table (@keysAndType, CONSTRAINT @fk_name FOREIGN KEY (@onlykeys) REFERENCES @schema.@ref_table(ref_field)";
            else 
                query += "ALTER TABLE @schema.@table @fieldChanges" +
                         "ADD CONSTRAINT @fk_name FOREIGN KEY (@onlyfields) REFERENCES @schema.@ref_table(@ref_field);";

            var cmd = MysqlManager.CreateTransaction();
            cmd.CommandText = query;
            cmd.SetParameter("@schema", table.Schema);
            cmd.SetParameter("@table", table.SqlName);
            cmd.SetParameter("@onlyfields", string.Join(",", manyToOne.Links.Select(link=>link.Key)));
            cmd.SetParameter("@keysAndType", string.Join(",", manyToOne.Links.Select(link=>BuildFkField(link,manyToOne))));
            cmd.SetParameter("@fk_name", manyToOne.FkName);
            cmd.SetParameter("@ref_table", manyToOne.TableReferenced.SqlName);
            cmd.SetParameter("@ref_field", string.Join(",", manyToOne.Links.Select(link=>link.Value.SqlName)));
            cmd.SetParameter("@fieldChanges", fieldChanges);
            
            try
            {
                Console.WriteLine(cmd.CommandText);
                cmd.ExecuteNonQuery();
                cmd.Transaction.Commit();
            }
            catch (Exception ex)
            {
                cmd.Transaction.Rollback();
                throw new MySqlConnectorException(
                    $"Error on create table {manyToOne.Table.SqlName} in database {manyToOne.Table.Schema}",
                    ex);
            }

            return true;
        }

        public static bool HasRows(string query)
        {
            var cmd = MysqlManager.GetConn().CreateCommand();
            cmd.CommandText = query;
            MySqlDataReader reader = null;
            try
            {
                reader = cmd.ExecuteReader();
                return reader.HasRows;
            }
            catch (Exception ex)
            {
                throw new MySqlConnectorException(
                    $"Error on execute select hasRows", ex);
            }
            finally
            {
                reader?.Close();
            }
        }

        public static void Execute(MySqlCommand cmd)
        {
            try
            {
                cmd.ExecuteNonQuery();
                cmd.Transaction.Commit();
            }
            catch (Exception ex)
            {
                cmd.Transaction.Rollback();
                throw new MySqlConnectorException(
                    $"Error Execute command {cmd.CommandText}",
                    ex);
            }
        }

        public static void ExecuteScript(string query)
        {
            try
            {
                var script = new MySqlScript(query);
                script.Connection = MysqlManager.GetConn();
                script.Delimiter = "//";
                script.Execute();
            }
            catch (Exception ex)
            {
                throw new MySqlConnectorException(
                    $"Error on execute script command {query}", ex);
            }

        }
    }
}