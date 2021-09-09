using Microsoft.Azure.Cosmos.Table;

using SujaySarma.Data.Azure.Tables.Relationships;

using System.Text;

namespace SujaySarma.Data.Azure.Tables.Commands
{

    /// <summary>
    /// Supports querying data from the Azure Storage Table service
    /// </summary>
    public sealed class Query
    {

        /// <summary>
        /// Partition Key to query by
        /// </summary>
        public string? PartitionKey { get; set; }

        /// <summary>
        /// Row Key to query by
        /// </summary>
        public string? RowKey { get; set; }

        /// <summary>
        /// The OData Where clause
        /// </summary>
        public string? ODataFilterString { get; set; }

        /// <summary>
        /// List of columns to fetch
        /// </summary>
        public IEnumerable<string>? Columns { get; set; }

        /// <summary>
        /// Name of column to order by
        /// </summary>
        public string? OrderBy { get; set; }

        /// <summary>
        /// If OrderBy is in descending order. Default: FALSE
        /// </summary>
        public bool IsOrderByDescending { get; set; } = false;

        /// <summary>
        /// Number of records to fetch. Value of -1 or zero will fetch all rows. 
        /// If for some reason you need to fetch more than int.MaxValue rows, just use -1 eh?
        /// </summary>
        public int Count { get; set; } = -1;

        /// <summary>
        /// True (default) if Table uses the IsDeleted column
        /// </summary>
        public bool UsesIsDeleted { get; set; } = true;

        /// <summary>
        /// False (default) if generated query should ignore the IsDeleted column
        /// </summary>
        public bool IgnoreIsDeleted { get; set; } = false;

        /// <summary>
        /// All the join/filters to be applied
        /// </summary>
        public IList<RelatedJoin> Joins { get; set; } = new List<RelatedJoin>();


        /// <summary>
        /// Initialize the structure
        /// </summary>
        /// <param name="tableUsesIsDeleted">TRUE if table uses the IsDeleted column to track soft-deletes</param>
        public Query(bool tableUsesIsDeleted = true) => UsesIsDeleted = tableUsesIsDeleted;

        /// <summary>
        /// Returns the equivalent TableQuery
        /// </summary>
        /// <returns>TableQuery to use with the ExecuteQuery call</returns>
        public TableQuery<Internal.CosmosDB.TableEntity> ToQuery()
        {
            StringBuilder query = new();
            if (UsesIsDeleted && (! IgnoreIsDeleted))
            {
                query.Append("IsDeleted eq false");
            }

            if (!string.IsNullOrWhiteSpace(PartitionKey))
            {
                if (query.Length > 0)
                {
                    query.Append(" and ");
                }
                query.Append($"PartitionKey eq '{PartitionKey}'");
            }

            if (!string.IsNullOrWhiteSpace(RowKey))
            {
                if (query.Length > 0)
                {
                    query.Append(" and ");
                }
                query.Append($"RowKey eq '{RowKey}'");
            }

            if (!string.IsNullOrWhiteSpace(ODataFilterString))
            {
                if (query.Length > 0)
                {
                    query.Append(" and ");
                }
                query.Append($"{ODataFilterString}");
            }

            TableQuery<Internal.CosmosDB.TableEntity> tableQuery = (new TableQuery<Internal.CosmosDB.TableEntity>()).Where(query.ToString());
            if (Columns != null)
            {
                List<string> columnNamesToReturn = new();
                foreach (string item in Columns)
                {
                    if ((!string.IsNullOrWhiteSpace(item)) && (!columnNamesToReturn.Contains(item)))
                    {
                        columnNamesToReturn.Add(item);
                    }
                }

                if (columnNamesToReturn.Count > 0)
                {
                    // Partition & Row key must always be selected, or we get weird results!
                    if (!columnNamesToReturn.Contains("PartitionKey"))
                    {
                        columnNamesToReturn.Add("PartitionKey");
                    }

                    if (!columnNamesToReturn.Contains("RowKey"))
                    {
                        columnNamesToReturn.Add("RowKey");
                    }

                    tableQuery.Select(columnNamesToReturn);
                }
            }

            if (!string.IsNullOrWhiteSpace(OrderBy))
            {
                tableQuery = (IsOrderByDescending ? tableQuery.OrderByDesc(OrderBy) : tableQuery.OrderBy(OrderBy));
            }

            if (Count > 0)
            {
                tableQuery = tableQuery.Take(Count);
            }

            return tableQuery;
        }

        /// <summary>
        /// Return the fully constructed filter string
        /// </summary>
        /// <returns>Filter string</returns>
        public override string ToString() => ToQuery().FilterString;
    }
}
