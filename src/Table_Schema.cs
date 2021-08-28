namespace MySqlConnector
{
    public class KEY_TABLE_SCHEMA
    {
        
        public string TABLE_NAME { get; set; }
        public string COLUMN_NAME { get; set; }
        public string DATA_TYPE { get; set; }
        public string COLUMN_TYPE { get; set; }
        public string CONSTRAINT_NAME { get; set; }
        public string REFERENCED_TABLE_SCHEMA { get; set; }
        public string REFERENCED_TABLE_NAME { get; set; }
        public string REFERENCED_COLUMN_NAME { get; set; }
        
    }
    public class COLUMN_TABLE_SCHEMA
    {
        public string COLUMN_NAME { get; set; }
        public string COLUMN_TYPE { get; set; }
    }
}