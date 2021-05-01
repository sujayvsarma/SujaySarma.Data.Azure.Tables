using Microsoft.Azure.Cosmos.Table;

using SujaySarma.Data.Azure.Tables.Commands;
using SujaySarma.Data.Azure.Tables.Internal.CosmosDB;
using SujaySarma.Data.Azure.Tables.Internal.Reflection;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace SujaySarma.Data.Azure.Tables
{
    /// <summary>
    /// The class that provides all the functionality to interact with Azure Storage Table service.
    /// </summary>
    public sealed class DataSource : IDisposable
    {
        #region Properties

        /// <summary>
        /// Reference to the Azure Storage Account class
        /// </summary>
        private StorageAccount StorageAccount { get; set; }

        /// <summary>
        /// Set this to FALSE to indicate that this table does NOT have an IsDeleted column. 
        /// If this is set and the table does not have the field, then SELECT queries will FAIL.
        /// </summary>
        private bool UsesIsDeleted { get; set; } = true;

        /// <summary>
        /// Name of the current table
        /// </summary>
        public string CurrentTableName { get; private set; }

        #endregion

        #region Methods

        /// <summary>
        /// Execute a query that returns data from the Azure Storage Table
        /// </summary>
        /// <param name="query">Query to execute</param>
        /// <returns>IEnumerable rows</returns>
        public IEnumerable<Internal.CosmosDB.TableEntity> ExecuteQuery(Query query)
        {
            TableQuery<Internal.CosmosDB.TableEntity> q = query.ToQuery();
            foreach (Internal.CosmosDB.TableEntity tableEntity in _currentTableReference.ExecuteQuery(q))
            {
                yield return tableEntity;
            }
        }

        /// <summary>
        /// Execute a query that returns data from the Azure Storage Table
        /// </summary>
        /// <typeparam name="T">Type of business object</typeparam>
        /// <param name="query">Query to execute</param>
        /// <returns>IEnumerable of business object instances</returns>
        public IEnumerable<T> ExecuteQuery<T>(Query query) where T : class, new()
        {
            TableQuery<Internal.CosmosDB.TableEntity> q = query.ToQuery();
            foreach (Internal.CosmosDB.TableEntity tableEntity in _currentTableReference.ExecuteQuery(q))
            {
                yield return tableEntity.To<T>();
            }
        }

        /// <summary>
        /// Execute a CRUD operation
        /// </summary>
        /// <typeparam name="T">Type of business object</typeparam>
        /// <param name="operation">The operation to execute (as an SDK Crud operation)</param>
        public void ExecuteNonQuery<T>(Crud<T> operation) where T : class, new()
        {
            switch (operation.CommandType)
            {
                case OperationType.Insert:
                    if (operation.UseBatches)
                    {
                        TableBatchOperation tbo = new();
                        foreach (T item in operation.Data)
                        {
                            tbo.Insert(Internal.CosmosDB.TableEntity.From(item));
                        }

                        ExecuteNonQuery(tbo);
                    }
                    else
                    {
                        foreach (T item in operation.Data)
                        {
                            ExecuteNonQuery(TableOperation.Insert(Internal.CosmosDB.TableEntity.From(item)));
                        }
                    }
                    break;


                case OperationType.Update:
                    if (operation.UseBatches)
                    {
                        TableBatchOperation tbo = new();
                        foreach (T item in operation.Data)
                        {
                            tbo.Merge(Internal.CosmosDB.TableEntity.From(item));
                        }

                        ExecuteNonQuery(tbo);
                    }
                    else
                    {
                        foreach (T item in operation.Data)
                        {
                            ExecuteNonQuery(TableOperation.Merge(Internal.CosmosDB.TableEntity.From(item)));
                        }
                    }
                    break;


                case OperationType.InsertOrMerge:
                    if (operation.UseBatches)
                    {
                        TableBatchOperation tbo = new();
                        foreach (T item in operation.Data)
                        {
                            tbo.InsertOrMerge(Internal.CosmosDB.TableEntity.From(item));
                        }

                        ExecuteNonQuery(tbo);
                    }
                    else
                    {
                        foreach (T item in operation.Data)
                        {
                            ExecuteNonQuery(TableOperation.InsertOrMerge(Internal.CosmosDB.TableEntity.From(item)));
                        }
                    }
                    break;


                case OperationType.InsertOrReplace:
                    if (operation.UseBatches)
                    {
                        TableBatchOperation tbo = new();
                        foreach (T item in operation.Data)
                        {
                            tbo.InsertOrReplace(Internal.CosmosDB.TableEntity.From(item));
                        }

                        ExecuteNonQuery(tbo);
                    }
                    else
                    {
                        foreach (T item in operation.Data)
                        {
                            ExecuteNonQuery(TableOperation.InsertOrReplace(Internal.CosmosDB.TableEntity.From(item)));
                        }
                    }
                    break;


                case OperationType.Replace:
                    if (operation.UseBatches)
                    {
                        TableBatchOperation tbo = new();
                        foreach (T item in operation.Data)
                        {
                            tbo.Replace(Internal.CosmosDB.TableEntity.From(item));
                        }

                        ExecuteNonQuery(tbo);
                    }
                    else
                    {
                        foreach (T item in operation.Data)
                        {
                            ExecuteNonQuery(TableOperation.Replace(Internal.CosmosDB.TableEntity.From(item)));
                        }
                    }
                    break;


                case OperationType.Delete:
                    Internal.CosmosDB.TableEntity entity;
                    if (operation.UseBatches)
                    {
                        TableBatchOperation tbo = new();
                        foreach (T item in operation.Data)
                        {
                            entity = Internal.CosmosDB.TableEntity.From(item);
                            if (UsesIsDeleted)
                            {
                                entity.AddOrUpdateProperty(Internal.CosmosDB.TableEntity.PROPERTY_NAME_ISDELETED, true);
                            }
                            tbo.Delete(entity);
                        }

                        ExecuteNonQuery(tbo);
                    }
                    else
                    {
                        foreach (T item in operation.Data)
                        {
                            entity = Internal.CosmosDB.TableEntity.From(item);
                            if (UsesIsDeleted)
                            {
                                entity.AddOrUpdateProperty(Internal.CosmosDB.TableEntity.PROPERTY_NAME_ISDELETED, true);
                            }

                            ExecuteNonQuery(TableOperation.Delete(entity));
                        }
                    }
                    break;
            }
        }

        /// <summary>
        /// Execute a CRUD operation
        /// </summary>
        /// <param name="operation">The operation to execute (as Azure TableBatchOperation)</param>
        public async void ExecuteNonQuery(TableBatchOperation operation)
        {
            // TBOs must be paged by partition key, and no more than a 100 items per batch page
            if (operation.Count > 0)
            {
                TableBatchOperation batchPage = new();

                // all entities in a batch must have the same partition key:
                foreach (IEnumerable<TableOperation> operations in operation.GroupBy(o => o.Entity.PartitionKey))
                {
                    // order elements in a partition by row key so that we reduce tablescans
                    foreach (TableOperation op in operations.OrderBy(o => o.Entity.RowKey))
                    {
                        batchPage.Add(op);
                        if (batchPage.Count == 100)
                        {
                            await _currentTableReference.ExecuteBatchAsync(batchPage);
                            batchPage.Clear();
                        }
                    }
                }

                // get the remaining
                if (batchPage.Count > 0)
                {
                    await _currentTableReference.ExecuteBatchAsync(batchPage);
                }
            }
        }

        /// <summary>
        /// Execute a CRUD operation
        /// </summary>
        /// <param name="operation">The operation to execute (as Azure TableOperation)</param>
        public async void ExecuteNonQuery(TableOperation operation)
        {
            await _currentTableReference.ExecuteAsync(operation);
        }

        /// <summary>
        /// Returns a list of tables
        /// </summary>
        /// <returns>IEnumerable of names of the tables present</returns>
        public IEnumerable<string> ListTables() => _currentTableClient.ListTables().Select(t => t.Name);

        /// <summary>
        /// Drops the current table
        /// </summary>
        public async void Drop() => await _currentTableReference.DeleteIfExistsAsync();

        /// <summary>
        /// Creates the table if it does not exist
        /// </summary>
        public async void Create() => await _currentTableReference.CreateIfNotExistsAsync();

        /// <summary>
        /// Returns if the current table exists
        /// </summary>
        /// <returns>True if table exists</returns>
        public async Task<bool> Exists() => await _currentTableReference.ExistsAsync();

        /// <summary>
        /// Clear all data for a particular PartitionKey value
        /// </summary>
        /// <param name="partitionKey">The PartitionKey to clear data for</param>
        public async void ClearPartition(string partitionKey)
        {
            if (string.IsNullOrWhiteSpace(partitionKey))
            {
                throw new ArgumentNullException(nameof(partitionKey));
            }

            TableBatchOperation deletes = new();
            foreach(Internal.CosmosDB.TableEntity entity in ExecuteQuery(new Query(false) { PartitionKey = partitionKey }))
            {
                deletes.Add(TableOperation.Delete(entity));
                if (deletes.Count == 100)
                {
                    await _currentTableReference.ExecuteBatchAsync(deletes);
                    deletes.Clear();
                }
            }

            if (deletes.Count > 0)
            {
                await _currentTableReference.ExecuteBatchAsync(deletes);
            }
        }


        /// <summary>
        /// Delete all the rows from the table
        /// </summary>
        public void DeleteAllRows()
        {
            Drop();

            // sleep to wait for the table to disappear
            System.Threading.Thread.Sleep(5000);

            Create();
        }


        #endregion


        #region IDisposable Support

        /// <summary>
        /// Dispose the datasource. Does not do anything, this is only so that we can use 
        /// this class in a 'using' statement like other datasource classes
        /// </summary>
        public void Dispose()
        {
            if (!alreadyDisposed)
            {
                alreadyDisposed = true;
            }
        }
        private bool alreadyDisposed = false; // To detect redundant calls

        #endregion

        /// <summary>
        /// Initialize the data source. Also ensures that the table exists.
        /// </summary>
        /// <param name="storageConnectionString">Storage connection string</param>
        /// <param name="tableName">Name of the table to connect to</param>
        /// <param name="usesIsDeleted">Set to FALSE for tables that do not have an IsDeleted field (i.e., external tables not created by this SDK)</param>
        public DataSource(string storageConnectionString, string tableName, bool usesIsDeleted = true)
        {
            if (string.IsNullOrWhiteSpace(storageConnectionString))
            {
                throw new ArgumentNullException(nameof(storageConnectionString));
            }

            if (string.IsNullOrWhiteSpace(tableName))
            {
                throw new ArgumentNullException(nameof(tableName));
            }

            StorageAccount = new StorageAccount(storageConnectionString);
            CurrentTableName = tableName;
            UsesIsDeleted = usesIsDeleted;

            _currentTableClient = new CloudTableClient(
                    StorageAccount.TableUri,
                    new StorageCredentials(StorageAccount.AccountName, StorageAccount.AccountKey)
                );

            _currentTableReference = _currentTableClient.GetTableReference(CurrentTableName);
            if (!_currentTableReference.Exists())
            {
                _currentTableReference.Create();
            }
        }

        /// <summary>
        /// Initialize the data source. Also ensures that the table exists.
        /// </summary>
        /// <param name="storageConnectionString">Storage connection string</param>
        /// <param name="businessObjectType">Type of business object</param>
        public DataSource(string storageConnectionString, Type businessObjectType)
        {
            Class? c = Reflector.InspectForAzureTables(businessObjectType);
            if (c == null)
            {
                throw new TypeLoadException($"Could not load type provided in '{nameof(businessObjectType)}'");
            }

            if (string.IsNullOrWhiteSpace(storageConnectionString))
            {
                throw new ArgumentNullException(nameof(storageConnectionString));
            }

            StorageAccount = new StorageAccount(storageConnectionString);
            CurrentTableName = c.TableAttribute.TableName;
            UsesIsDeleted = c.TableAttribute.UseSoftDelete;

            _currentTableClient = new CloudTableClient(
                    StorageAccount.TableUri,
                    new StorageCredentials(StorageAccount.AccountName, StorageAccount.AccountKey)
                );

            _currentTableReference = _currentTableClient.GetTableReference(CurrentTableName);
            if (!_currentTableReference.Exists())
            {
                _currentTableReference.Create();
            }
        }

        private readonly CloudTableClient _currentTableClient;
        private readonly CloudTable _currentTableReference;
    }
}
