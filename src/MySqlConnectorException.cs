using System;
using MySql.Data.MySqlClient;
using Persistence;

namespace MySqlConnector
{
    [Serializable]
    public class MySqlConnectorException : SQLException
    {
        public override int ErrorCode { get; set; }

        public MySqlConnectorException() : base()
        {

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