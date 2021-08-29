using System;
using MySql.Data.MySqlClient;
using Persistence;

namespace MySqlConnector
{
    [Serializable]
    public class MySqlConnectorException : Exception, SQLException
    {
        public int ErrorCode { get; set; }

        public MySqlConnectorException(){

        }

        public MySqlConnectorException(string message) : base(message)
        {
            
        }

        public MySqlConnectorException(string message, Exception inner) : base(message, inner)
        {
            if (inner is MySqlException mex)
            {
                ErrorCode = mex.ErrorCode;
            }
        }
        
    }

}