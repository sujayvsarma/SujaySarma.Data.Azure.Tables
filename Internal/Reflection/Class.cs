using SujaySarma.Data.Azure.Tables.Attributes;

namespace Internal.Reflection
{
    /// <summary>
    /// Metadata about a class.
    /// </summary>
    internal sealed class Class
    {
        /// <summary>
        /// Name of the class
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// The Table attribute
        /// </summary>
        public TableAttribute TableAttribute { get; private set; }

        /// <summary>
        /// Get/set if the class has a PartitionKey defined
        /// </summary>
        public bool HasPartitionKey { get; set; } = false;

        /// <summary>
        /// Get/set if the class has a RowKey defined
        /// </summary>
        public bool HasRowKey { get; set; } = false;

        /// <summary>
        /// Get/set if the class has a ETag property defined
        /// </summary>
        public bool HasETag { get; set; } = false;

        /// <summary>
        /// Get/set if the class has a Timestamp defined
        /// </summary>
        public bool HasTimestamp { get; set; } = false;

        /// <summary>
        /// A readonly list of the properties
        /// </summary>
        public IReadOnlyList<MemberProperty> Properties { get; private set; }

        /// <summary>
        /// A readonly list of the fields
        /// </summary>
        public IReadOnlyList<MemberField> Fields { get; private set; }

        /// <summary>
        /// A readonly list of both properties and fields
        /// </summary>
        public IReadOnlyList<MemberBase> Members { get; private set; }


        /// <summary>
        /// Initialize the structure for an object
        /// </summary>
        /// <param name="localName">Name of the class</param>
        /// <param name="table">The TableAttribute reference</param>
        /// <param name="properties">A readonly list of the properties in this object</param>
        /// <param name="fields">A readonly list of fields in this object</param>
        public Class(string localName, TableAttribute table, IReadOnlyList<MemberProperty> properties, IReadOnlyList<MemberField> fields)
        {
            Name = localName;
            TableAttribute = table;
            Properties = properties;
            Fields = fields;

            List<MemberBase> members = new();
            members.AddRange(properties);
            members.AddRange(fields);
            Members = members.AsReadOnly();

        }
    }
}
