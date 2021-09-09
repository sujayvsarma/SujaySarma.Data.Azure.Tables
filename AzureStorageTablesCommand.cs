
using Microsoft.Azure.Cosmos.Table;

using SujaySarma.Data.Azure.Tables.Commands;
using SujaySarma.Data.Azure.Tables.Internal.Reflection;
using SujaySarma.Data.Azure.Tables.Relationships;

namespace SujaySarma.Data.Azure.Tables
{
    /// <summary>
    /// The class that provides all the functionality to interact with Azure Storage Table service.
    /// </summary>
    public sealed class AzureStorageTablesCommand : IDisposable
    {
        #region Properties

        /// <summary>
        /// Reference to the Azure Storage Connection class
        /// </summary>
        private AzureStorageConnection Connection { get; set; }

        /// <summary>
        /// Set this to FALSE to indicate that this table does NOT have an IsDeleted column. 
        /// If this is set and the table does not have the field, then SELECT queries will FAIL.
        /// </summary>
        public bool UsesIsDeleted { get; set; } = true;

        #endregion

        #region Methods

        /// <summary>
        /// Execute a query that returns data from the Azure Storage Table
        /// </summary>
        /// <param name="query">Query to execute</param>
        /// <returns>IEnumerable rows</returns>
        public IEnumerable<Internal.CosmosDB.TableEntity> ExecuteQuery(Query query)
        {
            query.UsesIsDeleted = UsesIsDeleted;
            TableQuery<Internal.CosmosDB.TableEntity> q = query.ToQuery();

            foreach (Internal.CosmosDB.TableEntity tableEntity in Connection.ExecuteQuery(q))
            {
                bool retainRow = true;

                if (query.Joins.Count > 0)
                {
                    foreach(RelatedJoin join in query.Joins)
                    {
                        string filterWithLoopReplacements = join.ODataFilter;
                        filterWithLoopReplacements = filterWithLoopReplacements.Replace("$(PartitionKey)", tableEntity.PartitionKey);
                        filterWithLoopReplacements = filterWithLoopReplacements.Replace("$(RowKey)", tableEntity.RowKey);
                        filterWithLoopReplacements = filterWithLoopReplacements.Replace("$(Timestamp)", tableEntity.Timestamp.ToString());
                        filterWithLoopReplacements = filterWithLoopReplacements.Replace("$(ETag)", tableEntity.ETag);

                        foreach (string propertyName in tableEntity.Properties.Keys)
                        {
                            object? propertyValue = tableEntity.Properties[propertyName];
                            string str = ((propertyValue == null) ? "null" : (string)ReflectionUtils.GetAcceptableValue(propertyValue.GetType(), typeof(string), propertyValue)!);
                            filterWithLoopReplacements = filterWithLoopReplacements.Replace($"$({propertyName})", str);
                        }
                        filterWithLoopReplacements = filterWithLoopReplacements.Replace("\"null\"", "null");

                        using (AzureStorageConnection cn = new(Connection.ConnectionString))
                        {
                            cn.Open(join.TableType.TableAttribute.TableName, join.TableType.TableAttribute.UseSoftDelete, skipCreateCheck: true);
                            AzureStorageTablesCommand filterCmd = cn.CreateCommand();

                            bool filterHasRows = filterCmd.ExecuteQuery(new Query() { Count = 1, ODataFilterString = filterWithLoopReplacements }).Any();
                            cn.Close();

                            retainRow = retainRow && (((join.Type == RelatedJoinType.RetainWhenHasAny) && filterHasRows) || ((join.Type == RelatedJoinType.RetainWhenEmpty) && (! filterHasRows)));
                        }

                        if (! retainRow)
                        {
                            break;
                        }
                    }
                }

                if (retainRow)
                {
                    yield return tableEntity;
                }
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
            foreach (Internal.CosmosDB.TableEntity tableEntity in ExecuteQuery(query))
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
            // Don't use batches for small-sized requests
            // "5" is arbitrary
            if (operation.Data.Count() < 5)
            {
                operation.UseBatches = false;
            }

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
                                tbo.Replace(entity);
                            }
                            else
                            {
                                tbo.Delete(entity);
                            }
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
                                ExecuteNonQuery(TableOperation.Replace(entity));
                            }
                            else
                            {
                                ExecuteNonQuery(TableOperation.Delete(entity));
                            }
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
                            await Connection.ExecuteBatchAsync(batchPage);
                            batchPage.Clear();
                        }
                    }
                }

                // get the remaining
                if (batchPage.Count > 0)
                {
                    await Connection.ExecuteBatchAsync(batchPage);
                }
            }
        }

        /// <summary>
        /// Execute a CRUD operation
        /// </summary>
        /// <param name="operation">The operation to execute (as Azure TableOperation)</param>
        public async void ExecuteNonQuery(TableOperation operation)
        {
            await Connection.ExecuteAsync(operation);
        }

        /// <summary>
        /// Returns a list of tables
        /// </summary>
        /// <returns>IEnumerable of names of the tables present</returns>
        public IEnumerable<string> ListTables() => Connection.ListTables().Select(t => t.Name);

        /// <summary>
        /// Drops the current table
        /// </summary>
        public async void Drop() => await Connection.DropTable();

        /// <summary>
        /// Creates the table if it does not exist
        /// </summary>
        public async void Create() => await Connection.CreateTable();

        /// <summary>
        /// Returns if the current table exists
        /// </summary>
        /// <returns>True if table exists</returns>
        public async Task<bool> Exists() => await Connection.CheckTableExists();

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
                    await Connection.ExecuteBatchAsync(deletes);
                    deletes.Clear();
                }
            }

            if (deletes.Count > 0)
            {
                await Connection.ExecuteBatchAsync(deletes);
            }
        }


        /// <summary>
        /// Delete all the rows from the table
        /// </summary>
        public void ClearTable()
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
        /// Initializes using the provided connection
        /// </summary>
        /// <param name="connection">Storage connection to initialize with</param>
        /// <param name="usesIsDeleted">Set this to FALSE to indicate that this table does NOT have an IsDeleted column.</param>
        internal AzureStorageTablesCommand(AzureStorageConnection connection, bool usesIsDeleted = true)
        {
            Connection = connection;
            UsesIsDeleted = usesIsDeleted;
        }

    }
}
