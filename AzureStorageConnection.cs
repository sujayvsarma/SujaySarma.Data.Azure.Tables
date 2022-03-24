using Internal.Reflection;

using Microsoft.Azure.Cosmos.Table;

using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SujaySarma.Data.Azure.Tables
{

    /// <summary>
    /// Represents a connection to an Azure Storage account. This class cannot be inherited.
    /// </summary>
    public sealed class AzureStorageConnection : IDisposable
    {

        #region Properties

        /// <summary>
        /// Flag indicating if security information should be made available to callers. Default is FALSE.
        /// </summary>
        public bool PersistSecurityInfo { get; set; } = false;

        /// <summary>
        /// Credential used to establish connection. If PersistSecurityInfo is FALSE, then the 
        /// Getter will return NULL.
        /// </summary>
        public AzureStorageCredential? Credential
        {
            get => (PersistSecurityInfo ? _credential : null);
            set
            {
                if (value == null)
                {
                    throw new ArgumentNullException(nameof(Credential));
                }

                _credential = value;
            }
        }
        private AzureStorageCredential _credential;

        /// <summary>
        /// Transform the stored credential to a Cosmos SDK storage credential for use with the underlying connection.
        /// </summary>
        internal StorageCredentials StorageCredentials => new (_credential.AccountName, _credential.AccountSecret);

        /// <summary>
        /// The original connection string used
        /// </summary>
        public string ConnectionString { get; set; }

        /// <summary>
        /// Returns if connection is to a Development storage system
        /// </summary>
        public bool IsUsingDevelopmentStorage => (ConnectionString.Equals(UseDevelopmentStorage) || ConnectionString.Equals(DevelopmentStorageConnectionString));

        /// <summary>
        /// The table the connection is currently referencing. Will be NULL if <see cref="Open(string, bool)"/> has not been used.
        /// </summary>
        public string? TableName => _currentTableReference?.Name;

        /// <summary>
        /// Uri to the Table service (CosmosDB has the same scheme)
        /// </summary>
        public Uri AccountUri
            => (IsUsingDevelopmentStorage ? new Uri("http://127.0.0.1:10002/devstoreaccount1") : new Uri($"https://{_credential!.AccountName}.{TableHostname}.{HostnameDomainName}/"));


        #endregion

        #region Methods

        /// <summary>
        /// Open a connection to the specified table
        /// </summary>
        /// <param name="tableName">Name of the table to connect to</param>
        /// <param name="useIsDeleted">Set FALSE to disable use of IsDeleted column</param>
        /// <param name="skipCreateCheck">Set TRUE to skip automatic creation step</param>
        public void Open(string tableName, bool useIsDeleted = true, bool skipCreateCheck = false)
        {
            if (_currentTableReference != null)
            {
                throw new InvalidOperationException($"The connection is currently open to '{TableName}'. Close that connection or use ChangeTable().");
            }

            if (_tableClient == null)
            {
                _tableClient = new(AccountUri, StorageCredentials);
            }

            _currentTableReference = _tableClient.GetTableReference(tableName);
            _currentTableUsesIsDeleted = useIsDeleted;

            if (!skipCreateCheck)
            {
                _currentTableReference.CreateIfNotExists();
            }
        }

        /// <summary>
        /// Open a connection to the specified object
        /// </summary>
        /// <typeparam name="T">Type of business object</typeparam>
        /// <param name="skipCreateCheck">Set TRUE to skip automatic creation step</param>
        public void Open<T>(bool skipCreateCheck = false) where T : class, new()
        {
            Class? info = Reflector.InspectForAzureTables<T>();
            if ((info == null) || string.IsNullOrWhiteSpace(info.TableAttribute.TableName))
            {
                throw new TypeLoadException($"The type '{typeof(T).Name}' does not have a [Table] attribute.");
            }

            Open(info.TableAttribute.TableName, info.TableAttribute.UseSoftDelete, skipCreateCheck);
        }

        /// <summary>
        /// Close the currently open connection
        /// </summary>
        public void Close()
        {
            if (_currentTableReference != null)
            {
                _currentTableReference = null;
            }
        }

        /// <summary>
        /// Change connection to a different table
        /// </summary>
        /// <param name="tableName">Name of table to connect to</param>
        /// <param name="useIsDeleted">Set FALSE to disable use of IsDeleted column</param>
        public void ChangeTable(string tableName, bool useIsDeleted = true)
        {
            Close();
            Open(tableName, useIsDeleted);
        }

        /// <summary>
        /// Change table to the new object
        /// </summary>
        /// <typeparam name="T">Type of business object</typeparam>
        public void ChangeTable<T>() where T : class, new()
        {
            Class? info = Reflector.InspectForAzureTables<T>();
            if ((info == null) || string.IsNullOrWhiteSpace(info.TableAttribute.TableName))
            {
                throw new TypeLoadException($"The type '{typeof(T).Name}' does not have a [Table] attribute.");
            }
            
            _currentTableUsesIsDeleted = info.TableAttribute.UseSoftDelete;
            ChangeTable(info.TableAttribute.TableName, info.TableAttribute.UseSoftDelete);
        }

        /// <summary>
        /// Creates a storage tables command instance
        /// </summary>
        /// <returns>Instance of AzureStorageTablesCommand</returns>
        public AzureStorageTablesCommand CreateCommand()
        {
            if (_currentTableReference == null)
            {
                throw new InvalidOperationException("Use Open() before calling CreateCommand()");
            }
            return new AzureStorageTablesCommand(this, _currentTableUsesIsDeleted);
        }

        #endregion

        #region Internal Methods

        /// <summary>
        /// Execute the query against the open table connection
        /// </summary>
        /// <param name="query">TableQuery to execute</param>
        /// <returns>Enumerable of TableEntity</returns>
        internal IEnumerable<Internal.CosmosDB.TableEntity> ExecuteQuery(TableQuery<Internal.CosmosDB.TableEntity> query)
        {
            if (_currentTableReference == null)
            {
                throw new InvalidOperationException($"Open() has not been called.");
            }

            return _currentTableReference.ExecuteQuery(query);
        }

        /// <summary>
        /// Execute the query against the open table connection
        /// </summary>
        /// <param name="operation">TableQuery to execute</param>
        /// <returns>Enumerable of TableEntity</returns>
        internal async Task<TableResult> ExecuteAsync(TableOperation operation)
        {
            if (_currentTableReference == null)
            {
                throw new InvalidOperationException($"Open() has not been called.");
            }

            return await _currentTableReference.ExecuteAsync(operation);
        }

        /// <summary>
        /// Execute a batch asynchronously
        /// </summary>
        /// <param name="batchOperation">TableBatchOperation to execute async</param>
        /// <returns>Results of the batch operation</returns>
        internal async Task<TableBatchResult> ExecuteBatchAsync(TableBatchOperation batchOperation)
        {
            if (_currentTableReference == null)
            {
                throw new InvalidOperationException($"Open() has not been called.");
            }

            return await _currentTableReference.ExecuteBatchAsync(batchOperation);
        }

        /// <summary>
        /// Enumerates all tables in the current storage connection
        /// </summary>
        /// <returns>IEnumerable of CloudTables</returns>
        internal IEnumerable<CloudTable> ListTables()
        {
            if (_tableClient == null)
            {
                throw new InvalidOperationException($"Open() has not been called.");
            }

            return _tableClient.ListTables();
        }

        /// <summary>
        /// Drop the current table
        /// </summary>
        internal async Task<bool> DropTable()
        {
            if (_currentTableReference == null)
            {
                throw new InvalidOperationException($"Open() has not been called.");
            }

            return await _currentTableReference.DeleteIfExistsAsync();
        }

        /// <summary>
        /// Create the current table
        /// </summary>
        /// <returns>True if table was created</returns>
        internal async Task<bool> CreateTable()
        {
            if (_currentTableReference == null)
            {
                throw new InvalidOperationException($"Open() has not been called.");
            }

            return await _currentTableReference.CreateIfNotExistsAsync();
        }

        /// <summary>
        /// Check if the current table exists
        /// </summary>
        /// <returns>True if table exists</returns>
        internal async Task<bool> CheckTableExists()
        {
            if (_currentTableReference == null)
            {
                throw new InvalidOperationException($"Open() has not been called.");
            }

            return await _currentTableReference.ExistsAsync();
        }




        #endregion

        /// <summary>
        /// Instantiate the connection with defaults
        /// </summary>
        public AzureStorageConnection()
        {
            _credential = new();
            ConnectionString = DevelopmentStorageConnectionString;
        }

        /// <summary>
        /// Instantiate the connection with the provided connection string
        /// </summary>
        /// <param name="connectionString">Connection string to use to connect to the Azure Storage</param>
        public AzureStorageConnection(string connectionString) : this()
        {
            Credential = AzureStorageCredential.FromConnectionString(connectionString);
            ConnectionString = connectionString;
        }

        /// <summary>
        /// Instantiate the connection using the provided information
        /// </summary>
        /// <param name="accountName">Name of the Azure Storage account to connect to</param>
        /// <param name="accountKey">AccountKey of the Azure Storage account to authenticate to</param>
        public AzureStorageConnection(string accountName, string accountKey)
        {
            Credential = new AzureStorageCredential(accountName, accountKey);
            ConnectionString = $"DefaultEndpointsProtocol=https;AccountName={_credential!.AccountName};AccountKey={_credential!.AccountSecret};EndpointSuffix=core.windows.net";
        }


        #region IDisposable

        /// <summary>
        /// Dispose the connection
        /// </summary>
        public void Dispose()
        {
            if (!isDisposed)
            {
                isDisposed = true;

                Close();
            }
        }
        private bool isDisposed = false;
        #endregion

        private CloudTableClient? _tableClient;
        private CloudTable? _currentTableReference;
        internal bool _currentTableUsesIsDeleted = true;

        private const string UseDevelopmentStorage = "UseDevelopmentStorage=true";
        private const string DevelopmentStorageConnectionString = "DefaultEndpointsProtocol=https;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;EndpointSuffix=core.windows.net";
        private const string TableHostname = "table";
        private const string HostnameDomainName = "core.windows.net";

    }
}
