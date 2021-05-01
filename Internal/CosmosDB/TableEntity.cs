using Microsoft.Azure.Cosmos.Table;

using Newtonsoft.Json;

using SujaySarma.Data.Azure.Tables.Attributes;
using SujaySarma.Data.Azure.Tables.Internal.Helpers;
using SujaySarma.Data.Azure.Tables.Internal.Reflection;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace SujaySarma.Data.Azure.Tables.Internal.CosmosDB
{
    /// <summary>
    /// A TableEntity implementation that avoids us having to write TableEntity classes for every 
    /// business object we have in the system!
    /// </summary>
    public class TableEntity : ITableEntity
    {

        #region Properties

        /// <summary>
        /// The partition key
        /// </summary>
        public string PartitionKey
        {
            get => _partitionKey;
            set
            {
                if (!IsTableKeyValidFormat(value))
                {
                    throw new ArgumentException(nameof(PartitionKey));
                }

                _partitionKey = value;
            }
        }
        private string _partitionKey = string.Empty;

        /// <summary>
        /// The row key
        /// </summary>
        public string RowKey
        {
            get => _rowKey;
            set
            {
                if (!IsTableKeyValidFormat(value))
                {
                    throw new ArgumentException(nameof(RowKey));
                }

                _rowKey = value;
            }
        }
        private string _rowKey = string.Empty;

        /// <summary>
        /// The LastModified timestamp of the row
        /// </summary>
        public DateTimeOffset Timestamp { get; set; } = DateTimeOffset.MinValue;

        /// <summary>
        /// The E-Tag value of the row. Set to "*" to perform a blind update
        /// </summary>
        public string ETag { get; set; } = "*";

        #endregion

        #region Properties -- Expando Implementation

        private IDictionary<string, object?> _properties = new Dictionary<string, object?>();
        internal static string PROPERTY_NAME_ISDELETED = "IsDeleted";

        /// <summary>
        /// Adds or updates a property. If property already exists, updates the value. Otherwise adds a new property.
        /// </summary>
        /// <param name="name">Name of the property</param>
        /// <param name="value">Value of the property</param>
        public void AddOrUpdateProperty(string name, object? value)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (_properties.ContainsKey(name))
            {
                _properties[name] = value;
            }
            else
            {
                _properties.Add(name, value);
            }
        }

        /// <summary>
        /// Returns the value of the specified property
        /// </summary>
        /// <param name="name">Name of the property</param>
        /// <param name="defaultValue">Value to return if the property does not exist</param>
        /// <returns>Value of specified property</returns>
        public object? GetPropertyValue(string name, object? defaultValue = null)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                throw new ArgumentNullException(nameof(name));
            }

            if (!_properties.ContainsKey(name))
            {
                return defaultValue;
            }

            if (_properties[name] == null)
            {
                return defaultValue;
            }

            object? value = _properties[name];
            if (value == null)
            {
                return null;
            }

            if (value is EntityProperty property)
            {
                return property.PropertyAsObject;
            }

            return value;
        }

        #endregion

        #region Constructors

        /// <summary>
        /// Blank constructor, used by the rehydrator
        /// </summary>
        public TableEntity()
        {
            Timestamp = DateTimeOffset.UtcNow;
            AddOrUpdateProperty(PROPERTY_NAME_ISDELETED, false);

            // without this data cannot be updated
            ETag = "*";
        }

        /// <summary>
        /// Create a table entity from a business object
        /// </summary>
        /// <param name="instance">Business object instance</param>
        /// <param name="forDelete">If TRUE, only the partition and row key data is extracted. We don't waste cycles populating other properties</param>
        /// <typeparam name="T">Type of the business object instance</typeparam>
        /// <returns>The instantiated TableEntity</returns>
        public static TableEntity From<T>(T instance, bool forDelete = false)
            where T : class
        {
            if (instance == null)
            {
                throw new ArgumentNullException(nameof(instance));
            }

            TableEntity entity = new();

            Class? objectInfo = Reflector.InspectForAzureTables<T>();
            if (objectInfo == null)
            {
                throw new TypeLoadException($"Type '{typeof(T).FullName}' is not anotated with the '{typeof(TableAttribute).FullName}' attribute or has no properties/fields mapped to an Azure table.");
            }

            bool hasPartitionKey = false, hasRowKey = false, hasEtag = false;
            foreach (MemberBase member in objectInfo.Members)
            {
                object? value = member.Read(instance);

                if (member.IsPartitionKey)
                {
                    if (value == null)
                    {
                        throw new Exception("PartitionKey must be NON NULL.");
                    }

                    if (GetAcceptableValue(member.Type, typeof(string), value) is not string pk1)
                    {
                        throw new InvalidOperationException("PartitionKey cannot be NULL.");
                    }

                    entity.PartitionKey = pk1;
                    hasPartitionKey = true;
                }
                if (member.IsRowKey)
                {
                    if (value == null)
                    {
                        throw new Exception("RowKey must be NON NULL.");
                    }

                    if (GetAcceptableValue(member.Type, typeof(string), value) is not string rk1)
                    {
                        throw new InvalidOperationException("RowKey cannot be NULL.");
                    }

                    entity.RowKey = rk1;
                    hasRowKey = true;
                }
                if ((member.TableEntityColumn != null) && member.TableEntityColumn.ColumnName.Equals("ETag"))
                {
                    if (value == null)
                    {
                        value = "*";
                    }

                    if (GetAcceptableValue(member.Type, typeof(string), value) is not string etag)
                    {
                        throw new InvalidOperationException("ETag cannot be NULL.");
                    }

                    entity.ETag = etag;
                    hasEtag = true;
                }

                if ((!forDelete) && (member.TableEntityColumn != null))
                {
                    if ((!member.IsEdmType) && member.TableEntityColumn.JsonSerialize)
                    {
                        entity.AddOrUpdateProperty(
                                    member.TableEntityColumn.ColumnName,
                                    JsonConvert.SerializeObject(value)
                                );
                    }
                    else
                    {
                        entity.AddOrUpdateProperty(
                            member.TableEntityColumn.ColumnName,
                            GetAcceptableValue(member.Type, (member.IsEdmType ? member.Type : typeof(string)), value)
                        );
                    }
                }

                if (forDelete && hasPartitionKey && hasRowKey && ((objectInfo.HasETag && hasEtag) || (!objectInfo.HasETag)))
                {
                    break;
                }
            }

            return entity;
        }

        #endregion

        #region ITableEntity Methods

        /// <summary>
        /// Rehydrate the table entity from storage
        /// </summary>
        /// <param name="properties">The extra properties read from the Azure table storage</param>
        /// <param name="_">OperationContext, not used by this method</param>
        public void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext _)
        {
            _properties = new Dictionary<string, object?>();

            foreach (string propertyName in properties.Keys)
            {
                _properties.Add(propertyName, properties[propertyName]);
            }
        }

        /// <summary>
        /// Persist the table entity to storage
        /// </summary>
        /// <param name="_">OperationContext, not used by this method</param>
        /// <returns>Dictionary of properties to persist</returns>
        public IDictionary<string, EntityProperty> WriteEntity(OperationContext _)
        {
            Dictionary<string, EntityProperty> entityProperties = new();
            foreach (string propertyName in _properties.Keys)
            {
                entityProperties.Add(propertyName, EntityProperty.CreateEntityPropertyFromObject(_properties[propertyName]));
            }

            return entityProperties;
        }

        #endregion

        #region Methods

        /// <summary>
        /// Convert the current Table Entity object into the provided business object
        /// </summary>
        /// <typeparam name="T">Type of the business object instance</typeparam>
        /// <returns>Business object instance -- NULL if current entity is not properly populated.</returns>
        public T To<T>()
            where T : class, new()
        {
            Class? objectInfo = Reflector.InspectForAzureTables<T>();
            if (objectInfo == null)
            {
                throw new TypeLoadException($"Type '{typeof(T).Namespace}.{typeof(T).Name}' is not anotated with the '{typeof(TableAttribute).FullName}' attribute.");
            }

            T instance = new();
            foreach (MemberBase member in objectInfo.Members)
            {
                if (member.IsPartitionKey)
                {
                    member.Write(instance, GetAcceptableValue(typeof(string), member.Type, PartitionKey));
                }

                if (member.IsRowKey)
                {
                    member.Write(instance, GetAcceptableValue(typeof(string), member.Type, RowKey));
                }

                if (member.IsETag)
                {
                    member.Write(instance, GetAcceptableValue(typeof(string), member.Type, ETag));
                }

                if (member.IsTimestamp)
                {
                    member.Write(instance, GetAcceptableValue(typeof(DateTimeOffset), member.Type, Timestamp));
                }

                if (member.TableEntityColumn != null)
                {
                    object? value = GetPropertyValue(member.TableEntityColumn.ColumnName, default);
                    if ((value == default) || (value == null))
                    {
                        member.Write(instance, default);
                    }
                    else
                    {
                        if ((value.GetType() == typeof(string)) && (member.Type != typeof(string)) && member.TableEntityColumn.JsonSerialize)
                        {
                            member.Write(instance, JsonConvert.DeserializeObject((string)value, member.Type));
                        }
                        else
                        {
                            member.Write(instance, GetAcceptableValue(value.GetType(), member.Type, value));
                        }
                    }
                }
            }

            return instance;
        }

        /// <summary>
        /// Converts the provided list of table entity records to a list of business objects
        /// </summary>
        /// <typeparam name="T">Type of the business object instance</typeparam>
        /// <param name="records">AzureTableEntity records to process</param>
        /// <returns>List of business objects. An empty list if records were empty</returns>
        public static List<T> ToList<T>(List<TableEntity> records)
            where T : class, new()
        {
            Class? objectInfo = Reflector.InspectForAzureTables<T>();
            if (objectInfo == null)
            {
                throw new TypeLoadException($"Type '{typeof(T).Namespace}.{typeof(T).Name}' is not anotated with the '{typeof(TableAttribute).Namespace}.{typeof(TableAttribute).Name}' attribute.");
            }

            List<T> list = new();
            if ((records != null) && (records.Count > 0))
            {
                foreach (TableEntity tableEntity in records)
                {
                    T instance = new();

                    foreach (MemberBase member in objectInfo.Members)
                    {
                        if (member.IsPartitionKey)
                        {
                            member.Write(instance, GetAcceptableValue(typeof(string), member.Type, tableEntity.PartitionKey));
                        }

                        if (member.IsRowKey)
                        {
                            member.Write(instance, GetAcceptableValue(typeof(string), member.Type, tableEntity.RowKey));
                        }

                        if (member.IsETag)
                        {
                            member.Write(instance, GetAcceptableValue(typeof(string), member.Type, tableEntity.ETag));
                        }

                        if (member.IsTimestamp)
                        {
                            member.Write(instance, GetAcceptableValue(typeof(DateTimeOffset), member.Type, tableEntity.Timestamp));
                        }

                        if (member.TableEntityColumn != null)
                        {
                            object? value = tableEntity.GetPropertyValue(member.TableEntityColumn.ColumnName, default);
                            if ((value == default) || (value == null))
                            {
                                member.Write(instance, default);
                            }
                            else
                            {
                                if ((value.GetType() == typeof(string)) && (member.Type != typeof(string)) && member.TableEntityColumn.JsonSerialize)
                                {
                                    member.Write(instance, JsonConvert.DeserializeObject((string)value, member.Type));
                                }
                                else
                                {
                                    member.Write(instance, GetAcceptableValue(value.GetType(), member.Type, value));
                                }
                            }
                        }
                    }

                    list.Add(instance);
                }
            }

            return list;
        }

        /// <summary>
        /// Overwrites existing properties with values from the <paramref name="entity"/>. Properties that do not 
        /// exist in <paramref name="entity"/> are not touched.
        /// </summary>
        /// <param name="entity">The entity to copy values from</param>
        public void ImportValues(TableEntity entity)
        {
            PartitionKey = entity.PartitionKey;
            RowKey = entity.RowKey;

            // Should we copy Timestamp and Etag ???
            //Timestamp = entity.Timestamp;
            //ETag = entity.ETag;

            List<string> propertyNames = new();
            foreach (string key in _properties.Keys)
            {
                propertyNames.Add(key);
            }

            foreach (string propertyName in propertyNames)
            {
                if (entity._properties.ContainsKey(propertyName))
                {
                    _properties[propertyName] = entity._properties[propertyName];
                }
            }
        }

        #endregion


        #region Private helpers

        /// <summary>
        /// Returns a value that matches the destination type
        /// </summary>
        /// <param name="sourceType">Type of value being provided</param>
        /// <param name="destinationType">Type of the destination container</param>
        /// <param name="value">Value to convert/change</param>
        /// <returns>The value of type destinationType</returns>
        private static object? GetAcceptableValue(Type sourceType, Type destinationType, object? value)
        {
            Type? srcActualType = Nullable.GetUnderlyingType(sourceType);
            Type convertFromType = srcActualType ?? sourceType;

            Type? destActualType = Nullable.GetUnderlyingType(destinationType);
            Type convertToType = destActualType ?? destinationType;

            if (value == null)
            {
                return null;
            }

            if (Edm.NeedsConversion(convertFromType, convertToType))
            {
                return Edm.ConvertTo(convertToType, value);
            }

            return value;
        }

        /// <summary>
        /// Check if the proposedValue is valid as a partition/row key
        /// </summary>
        /// <param name="proposedValue">Value to check (string)</param>
        /// <returns>True if value can be used</returns>
        private static bool IsTableKeyValidFormat(string proposedValue)
        {
            return (!TableKeysValidationRegEx.IsMatch(proposedValue));
        }
        private static readonly Regex TableKeysValidationRegEx = new(@"[\\\\#%+/?\u0000-\u001F\u007F-\u009F]");


        #endregion
    }
}
