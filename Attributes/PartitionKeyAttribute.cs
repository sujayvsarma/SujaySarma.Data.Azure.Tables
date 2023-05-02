using System;

namespace SujaySarma.Data.Azure.Tables.Attributes
{
    /// <summary>
    /// Marks the property or field as the Partition Key for the table.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false, Inherited = true)]
    public sealed class PartitionKeyAttribute : AttributeBase
    {

        /// <summary>
        /// Will be used as the default value if one is not provided by the row.
        /// </summary>
        public string DefaultValue { get; set; } = "PK";
    }
}
