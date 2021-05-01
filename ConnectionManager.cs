using SujaySarma.Data.Azure.Tables.Attributes;
using SujaySarma.Data.Azure.Tables.Internal.Reflection;

using System;
using System.Collections.Concurrent;

namespace SujaySarma.Data.Azure.Tables
{
    /// <summary>
    /// Provides pooled connections to Azure Storage Tables
    /// </summary>
    public static class ConnectionManager
    {

        /// <summary>
        /// Gets a connection to an Azure Storage Table
        /// </summary>
        /// <typeparam name="T">Type of business object to fetch a connection for</typeparam>
        /// <returns>A DataSource connection</returns>
        public static DataSource GetOrAdd<T>() where T : class, new()
        => tables.GetOrAdd(
                    GetTableName<T>(),
                    (tableName) =>
                    {
                        return new DataSource(ConnectionString, typeof(T));
                    }
                );

        /// <summary>
        /// Get the tablename for the provided object
        /// </summary>
        /// <typeparam name="T">Type of business object</typeparam>
        /// <returns>Name of the associated Azure Storage table</returns>
        public static string GetTableName<T>() where T : class, new()
             => entityTableNames.GetOrAdd(
#pragma warning disable CS8604 // Possible null reference argument.
                    typeof(T).FullName,
#pragma warning restore CS8604 // Possible null reference argument.
                    (objectName) =>
                    {
                        Class? objectInfo = Reflector.InspectForAzureTables<T>();
                        if (objectInfo == null)
                        {
                            throw new TypeLoadException($"Type '{typeof(T).FullName}' is not anotated with the '{typeof(TableAttribute).FullName}' attribute.");
                        }

                        return objectInfo.TableAttribute.TableName;
                    }
                );

        /// <summary>
        /// Clears all cached connections
        /// </summary>
        public static void Clear() => tables.Clear();

        /// <summary>
        /// Initialize the connection string
        /// </summary>
        /// <param name="connectionString">Connection string to use for further connections</param>
        public static void Initialize(string connectionString) => ConnectionString = connectionString;


        private static string ConnectionString = "UseDevelopmentStorage=true";
        private static readonly ConcurrentDictionary<string, DataSource> tables = new();
        private static readonly ConcurrentDictionary<string, string> entityTableNames = new();

    }
}
