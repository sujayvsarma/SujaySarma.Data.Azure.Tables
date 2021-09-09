using Internal.Reflection;

namespace SujaySarma.Data.Azure.Tables.Relationships
{
    /// <summary>
    /// A join condition - used in the Commands.Query class
    /// </summary>
    public class RelatedJoin
    {
        /// <summary>
        /// Type of join
        /// </summary>
        public RelatedJoinType Type { get; set; } = RelatedJoinType.RetainWhenHasAny;

        /// <summary>
        /// The filter string
        /// </summary>
        public string ODataFilter { get; set; } = string.Empty;

        /// <summary>
        /// Type of the table object
        /// </summary>
        internal Class TableType { get; init; }

        /// <summary>
        /// Construct a join
        /// </summary>
        /// <param name="relatedTableType">Type of related table</param>
        /// <param name="type">Type of join (inner / outer)</param>
        /// <param name="filter">OData Filter string to use</param>
        public RelatedJoin(Type relatedTableType, RelatedJoinType type, string filter)
        {
            TableType = Reflector.InspectForAzureTables(relatedTableType) ?? throw new ArgumentException($"Type '{nameof(relatedTableType)}' cannot be used.");
            Type = type;
            ODataFilter = filter;
        }

    }

    /// <summary>
    /// Type of join to be used
    /// </summary>
    public enum RelatedJoinType
    {
        /// <summary>
        /// (default) Retain rows when filter returns at least one result
        /// </summary>
        RetainWhenHasAny = 0,

        /// <summary>
        /// Retain rows when filter returns nothing
        /// </summary>
        RetainWhenEmpty
    }
}
