namespace SujaySarma.Data.Azure.Tables.Commands
{
    /// <summary>
    /// Defines a CRUD (Create, Replace, Update, Delete) operation on the Azure Storage Table
    /// </summary>
    public class Crud<T> where T : class, new()
    {
        /// <summary>
        /// Type of operation to perform
        /// </summary>
        public OperationType CommandType { get; set; } = OperationType.Unknown;

        /// <summary>
        /// If set, and data contains more than one element then use TableBatchOperation to 
        /// execute optimally
        /// </summary>
        public bool UseBatches { get; set; } = true;

        /// <summary>
        /// Data to set in the Azure Table Storage
        /// </summary>
        public IEnumerable<T> Data { get; set; }

        /// <summary>
        /// Initialize
        /// </summary>
        /// <param name="type">Type of operation to perform</param>
        /// <param name="data">Data to set in the Azure Table Storage</param>
        public Crud(OperationType type, IEnumerable<T> data)
        {
            CommandType = type;
            Data = data;
        }

        /// <summary>
        /// Initialize
        /// </summary>
        /// <param name="type">Type of operation to perform</param>
        /// <param name="data">Data to set in the Azure Table Storage</param>
        public Crud(OperationType type, T data)
        {
            CommandType = type;
            Data = new[] { data };
        }



    }

    /// <summary>
    /// Type of CRUD operation to be performed
    /// </summary>
    public enum OperationType
    {
        /// <summary>
        /// Unknown operation
        /// </summary>
        Unknown = 0,

        /// <summary>
        /// Insert new rows
        /// </summary>
        Insert,

        /// <summary>
        /// Update existing rows
        /// </summary>
        Update,

        /// <summary>
        /// Insert or update rows
        /// </summary>
        InsertOrMerge,

        /// <summary>
        /// Insert or Replace rows
        /// </summary>
        InsertOrReplace,

        /// <summary>
        /// Replace rows
        /// </summary>
        Replace,

        /// <summary>
        /// Delete existing rows
        /// </summary>
        Delete
    }
}
