using SujaySarma.Data.Azure.Tables.Attributes;
using SujaySarma.Data.Azure.Tables.Internal.Helpers;

using System.Reflection;

namespace Internal.Reflection
{
    /// <summary>
    /// Base class implementing functionality common to class fields and properties
    /// </summary>
    internal class MemberBase
    {

        /// <summary>
        /// Name of the member
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Flag indicating if the member can be written (has a Setter)
        /// </summary>
        public bool CanWrite { get; private set; } = true;

        /// <summary>
        /// Data Type of the member
        /// </summary>
        public Type Type { get; private set; } = typeof(object);

        /// <summary>
        /// If true, the Type is supported by Edm
        /// </summary>
        public bool IsEdmType { get; private set; } = false;

        /// <summary>
        /// If true, the CLR type allows NULLs
        /// </summary>
        public bool IsNullableType { get; private set; } = true;

        /// <summary>
        /// Maps to the TableEntity's PartitionKey
        /// </summary>
        public bool IsPartitionKey { get; private set; } = false;

        /// <summary>
        /// Maps to the TableEntity's RowKey
        /// </summary>
        public bool IsRowKey { get; set; } = false;

        /// <summary>
        /// Maps to the TableEntity's ETag property
        /// </summary>
        public bool IsETag { get; set; } = false;

        /// <summary>
        /// Maps to the TableEntity's Timestamp property
        /// </summary>
        public bool IsTimestamp { get; set; } = false;


        /// <summary>
        /// If mapped to a TableEntity's fields, reference to the TableColumnAttribute for that.
        /// (Could be NULL)
        /// </summary>
        public TableColumnAttribute? TableEntityColumn { get; set; }

        /// <summary>
        /// Initialize the structure
        /// </summary>
        /// <param name="property">The property</param>
        protected MemberBase(System.Reflection.PropertyInfo property)
        {
            Name = property.Name;
            CanWrite = property.CanWrite;
            CommonInit(property, property.PropertyType);
        }

        /// <summary>
        /// Initialize the structure
        /// </summary>
        /// <param name="field">The field</param>
        protected MemberBase(System.Reflection.FieldInfo field)
        {
            Name = field.Name;
            CanWrite = (!field.IsInitOnly);
            CommonInit(field, field.FieldType);
        }

        /// <summary>
        /// Common initialization
        /// </summary>
        /// <param name="member">MemberInfo</param>
        /// <param name="dataType">Type of property/field</param>
        private void CommonInit(System.Reflection.MemberInfo member, Type dataType)
        {
            Type = dataType;

            foreach (Attribute attribute in member.GetCustomAttributes(true))
            {
                if (attribute is PartitionKeyAttribute)
                {
                    IsPartitionKey = true;
                }
                else if (attribute is RowKeyAttribute)
                {
                    IsRowKey = true;
                }
                else if (attribute is ETagAttribute)
                {
                    IsETag = true;
                }
                else if (attribute is TimestampAttribute)
                {
                    IsTimestamp = true;
                }
                else if (attribute is TableColumnAttribute tc)
                {
                    // Now the problem is legacy code that may define columns we now have attributes for 
                    // as TableColumn() with matching names. We do not want those anymore!
                    if (FixedColumnNames.Contains(tc.ColumnName))
                    {
                        throw new TypeLoadException($"{member.Name} is mapped to {tc.ColumnName} that has its own attribute. Use the '{tc.ColumnName}' attribute instead of specifying it as a column name!");
                    }

                    // Traditional column
                    TableEntityColumn = tc;
                }
            }

            Type? underlyingType = Nullable.GetUnderlyingType(Type);
            IsNullableType = (dataType == typeof(string)) || (underlyingType == typeof(string)) || (underlyingType != null);
            IsEdmType = Edm.IsEdmCompatibleType(underlyingType ?? Type);            
        }

        /// <summary>
        /// Read the value from the property/field and return it
        /// </summary>
        /// <typeparam name="ObjType">Data type of the object to read the property/field of</typeparam>
        /// <param name="obj">The object instance to read the value from</param>
        /// <returns>The value</returns>
        public virtual object? Read<ObjType>(ObjType obj) => null;

        /// <summary>
        /// Writes the provided value to the property/field of the object instance
        /// </summary>
        /// <typeparam name="ObjType">Data type of the object to read the property/field of</typeparam>
        /// <param name="obj">The object instance to write the value to</param>
        /// <param name="value">Value to write out</param>
        public virtual void Write<ObjType>(ObjType obj, object? value) { }


        /// <summary>
        /// Binding flags for read and write of properties/fields
        /// </summary>
        protected readonly BindingFlags FLAGS_READ_WRITE = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static;

        /// <summary>
        /// Names of columns that are fixed in Azure Tables
        /// </summary>
        private readonly string[] FixedColumnNames = new string[]
        {
            "PartitionKey", "RowKey", "Timestamp", "ETag"
        };
    }
}
