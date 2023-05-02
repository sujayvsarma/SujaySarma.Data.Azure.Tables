using System;

namespace SujaySarma.Data.Azure.Tables.Attributes
{
    /// <summary>
    /// Marks the property or field as the Row Key for the table.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, AllowMultiple = false, Inherited = true)]
    public sealed class RowKeyAttribute : AttributeBase
    {
        /// <summary>
        /// If set, and if the underlying type is guid or string, will generate a new Guid before storage.
        /// </summary>
        public bool AutogenerateGuid { get; set; } = true;
    }
}
