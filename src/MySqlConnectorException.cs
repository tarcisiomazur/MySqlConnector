using System;
using System.Runtime.Serialization;

namespace MySqlConnector
{
    [Serializable]
    public class MySqlConnectorException : Exception
    {

        public MySqlConnectorException(){

    }

        public MySqlConnectorException(string message) : base(message)
        {
            
        }

        public MySqlConnectorException(string message, Exception inner) : base(message, inner)
        {
            
        }

        protected MySqlConnectorException(
            SerializationInfo info,
            StreamingContext context) : base(info, context)
        {
        }
    }

}