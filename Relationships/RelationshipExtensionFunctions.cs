using Internal.Reflection;

using SujaySarma.Data.Azure.Tables.Commands;
using SujaySarma.Data.Azure.Tables.Internal.Reflection;

using System;
using System.Collections.Generic;
using System.Linq;

namespace SujaySarma.Data.Azure.Tables.Relationships
{
    /// <summary>
    /// Functions that simulate relationships between two tables
    /// </summary>
    public static class RelationshipExtensionFunctions
    {

        /// <summary>
        /// Perform an innerjoin between two tables. Returns only intersecting values
        /// </summary>
        /// <typeparam name="T">The main table -- only data from this table will be returned</typeparam>
        /// <typeparam name="RelationT">The related table - used only for filtering the rows in T</typeparam>
        /// <param name="enumerable">Values from the data table</param>
        /// <param name="odataFilter">OData filter string to fetch values for filter table</param>
        /// <param name="connection">The Table connection to use</param>
        /// <returns>Filtered data in Enumerable-pattern</returns>
        public static IEnumerable<T> With<T, RelationT>(this IEnumerable<T> enumerable, string odataFilter, AzureStorageConnection connection)
            where RelationT: class, new()
            where T: class, new()
        {
            bool filterHasItems;
            string filterWithLoopReplacements;
            Class? reflRelation = Reflector.InspectForAzureTables<RelationT>(), reflT = Reflector.InspectForAzureTables<T>();
            if (reflT == default)
            {
                throw new ArgumentException($"The type '{typeof(T).FullName}' cannot be used with this method.");
            }
            if (reflRelation == default)
            {
                throw new ArgumentException($"The type '{typeof(RelationT).FullName}' cannot be used with this method.");
            }

            AzureStorageConnection cn = new(connection.ConnectionString);
            cn.Open<RelationT>();
            using (AzureStorageTablesCommand cmd = cn.CreateCommand())
            {
                foreach (T sItem in enumerable)
                {
                    filterWithLoopReplacements = odataFilter;
                    foreach (MemberBase member in reflT.Members)
                    {
                        if (member.IsETag || member.IsPartitionKey || member.IsRowKey || member.IsTimestamp || (member.TableEntityColumn != null))
                        {
                            string str = (string?)ReflectionUtils.GetAcceptableValue(member.Type, typeof(string), member.Read(sItem)) ?? "null";
                            filterWithLoopReplacements = filterWithLoopReplacements.Replace($"$({member.Name})", str);
                        }
                    }
                    filterWithLoopReplacements = filterWithLoopReplacements.Replace("\"null\"", "null");

                    filterHasItems = cmd.ExecuteQuery(new Query() { Count = 1, ODataFilterString = filterWithLoopReplacements }).Any();
                    if (filterHasItems)
                    {
                        yield return sItem;
                    }
                }
            }
            cn.Close();
        }

        /// <summary>
        /// Perform an outerjoin between two tables. Returns only values that don't match the filter
        /// </summary>
        /// <typeparam name="T">The main table -- only data from this table will be returned</typeparam>
        /// <typeparam name="RelationT">The related table - used only for filtering the rows in T</typeparam>
        /// <param name="enumerable">Values from the data table</param>
        /// <param name="odataFilter">OData filter string to fetch values for filter table</param>
        /// <param name="connection">The Table connection to use</param>
        /// <returns>Filtered data in Enumerable-pattern</returns>
        public static IEnumerable<T> Without<T, RelationT>(this IEnumerable<T> enumerable, string odataFilter, AzureStorageConnection connection)
            where RelationT : class, new()
            where T : class, new()
        {
            bool filterHasItems;
            string filterWithLoopReplacements;
            Class? reflRelation = Reflector.InspectForAzureTables<RelationT>(), reflT = Reflector.InspectForAzureTables<T>();
            if (reflT == default)
            {
                throw new ArgumentException($"The type '{typeof(T).FullName}' cannot be used with this method.");
            }
            if (reflRelation == default)
            {
                throw new ArgumentException($"The type '{typeof(RelationT).FullName}' cannot be used with this method.");
            }

            AzureStorageConnection cn = new(connection.ConnectionString);
            cn.Open<RelationT>();
            using (AzureStorageTablesCommand cmd = cn.CreateCommand())
            {
                foreach (T sItem in enumerable)
                {
                    filterWithLoopReplacements = odataFilter;
                    foreach (MemberBase member in reflT.Members)
                    {
                        if (member.IsETag || member.IsPartitionKey || member.IsRowKey || member.IsTimestamp || (member.TableEntityColumn != null))
                        {
                            string str = (string?)ReflectionUtils.GetAcceptableValue(member.Type, typeof(string), member.Read(sItem)) ?? "null";
                            filterWithLoopReplacements = filterWithLoopReplacements.Replace($"$({member.Name})", str);
                        }
                    }
                    filterWithLoopReplacements = filterWithLoopReplacements.Replace("\"null\"", "null");

                    filterHasItems = cmd.ExecuteQuery(new Query() { Count = 1, ODataFilterString = filterWithLoopReplacements }).Any();
                    if (!filterHasItems)
                    {
                        yield return sItem;
                    }
                }
            }
            cn.Close();
        }


    }
}
