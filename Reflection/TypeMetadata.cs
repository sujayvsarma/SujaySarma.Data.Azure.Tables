using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text.Json;

using Azure;
using Azure.Data.Tables;

using SujaySarma.Data.Azure.Tables.Attributes;

namespace SujaySarma.Data.Azure.Tables.Reflection
{
    /// <summary>
    /// Metadata collected about a business object
    /// </summary>
    internal class TypeMetadata
    {

        /// <summary>
        /// Name of the table for the object
        /// </summary>
        public string TableName { get; set; } = default!;

        /// <summary>
        /// If the table uses soft-delete: TRUE if not set.
        /// </summary>
        public bool UseSoftDelete { get; set; } = true;

        /// <summary>
        /// The partition key property/field
        /// </summary>
        public MemberInfo? PartitionKey { get; set; }

        /// <summary>
        /// The row key property/field
        /// </summary>
        public MemberInfo? RowKey { get; set; }

        /// <summary>
        /// The ETag property/field
        /// </summary>
        public MemberInfo? ETag { get; set; }

        /// <summary>
        /// Other properties of the class
        /// </summary>
        public List<MemberInfo> Members { get; set; } = new();
        
        /// <summary>
        /// Transform a TableEntity to T
        /// </summary>
        /// <typeparam name="T">Type of business object</typeparam>
        /// <param name="entity">Table entity instance with values</param>
        /// <returns>Instantiated business object</returns>
        public T Transform<T>(TableEntity entity)
        {
            T instance = Activator.CreateInstance<T>();
            if (PartitionKey != default) ReflectionUtils.SetValueFromEdm(instance, PartitionKey, entity.PartitionKey);
            if (RowKey != default) ReflectionUtils.SetValueFromEdm(instance, RowKey, entity.RowKey);
            if (ETag != default) ReflectionUtils.SetValue(instance, ETag, entity.ETag.ToString("G"));

            foreach(MemberInfo member in Members)
            {
                if (RESERVED_MEMBER_NAMES.Contains(member.Name))
                {
                    continue;   // already set above
                }

                // we don't add to the collection unless we have the TC defined.
                TableColumnAttribute attribute = member.GetCustomAttribute<TableColumnAttribute>()!;
                object? value = entity[attribute.ColumnName];
                if (attribute.JsonSerialize)
                {
                    if ((value is string s) && (!string.IsNullOrWhiteSpace(s)))
                    {
#pragma warning disable 8509
                        ReflectionUtils.SetValueFromEdm(instance, member, JsonSerializer.Deserialize(s, member.MemberType switch
                        {
                            MemberTypes.Field => (member as FieldInfo)!.FieldType,
                            MemberTypes.Property => (member as PropertyInfo)!.PropertyType
                        }));
#pragma warning restore 8509
                    }
                }
                else
                {
                    ReflectionUtils.SetValueFromEdm(instance, member, value);
                }
            }

            return instance;
        }

        /// <summary>
        /// Transform a T to a TableEntity
        /// </summary>
        /// <typeparam name="T">Type of business object</typeparam>
        /// <param name="instance">Instance of business object</param>
        /// <param name="minimalSurface">If set, only populates PartitionKey, RowKey and ETag</param>
        /// <returns>TableEntity</returns>
        /// <exception cref="ArgumentException"></exception>
        public TableEntity Transform<T>(T instance, bool minimalSurface = false)
        {
            TableEntity entity = new();

            if (UseSoftDelete)
            {
                entity[ISDELETED_COLUMN_NAME] = false;
            }

            if (PartitionKey != default)
            {
                string? value = ReflectionUtils.GetValue<T, string>(instance, PartitionKey);
                if (value == default)
                {
                    PartitionKeyAttribute pk = PartitionKey.GetCustomAttribute<PartitionKeyAttribute>()!;
                    if (string.IsNullOrWhiteSpace(pk.DefaultValue))
                    {
                        throw new ArgumentException($"PartitionKey ({PartitionKey.Name}) for '{typeof(T).Name}' does not define values.");
                    }
                    value = pk.DefaultValue;
                }

                entity.PartitionKey = value;
            }

            if (RowKey != default)
            {
                string? value = ReflectionUtils.GetValue<T, string>(instance, RowKey);
                if (value == default)
                {
                    RowKeyAttribute rk = RowKey.GetCustomAttribute<RowKeyAttribute>()!;
                    if (!rk.AutogenerateGuid)
                    {
                        throw new ArgumentException($"RowKey ({RowKey.Name}) for '{typeof(T).Name}' does not define values.");
                    }
                    value = Guid.NewGuid().ToString("d");
                }

                entity.RowKey = value;
            }

            if (ETag != default)
            {
                entity.ETag = new ETag(ReflectionUtils.GetValue<T, string>(instance, ETag) ?? "*");
            }

            if (!minimalSurface)
            {
                foreach (MemberInfo member in Members)
                {
                    if (RESERVED_MEMBER_NAMES.Contains(member.Name))
                    {
                        continue;   // already set above
                    }

                    // we don't add to the collection unless we have the TC defined.
                    TableColumnAttribute attribute = member.GetCustomAttribute<TableColumnAttribute>()!;
                    object? value = ReflectionUtils.GetValue<T>(instance, member);
                    if (attribute.JsonSerialize)
                    {
                        if (value != default)
                        {
#pragma warning disable 8509
                            entity[attribute.ColumnName] = JsonSerializer.Serialize(
                                value,
                                member.MemberType switch
                                {
                                    MemberTypes.Field => (member as FieldInfo)!.FieldType,
                                    MemberTypes.Property => (member as PropertyInfo)!.PropertyType
                                }
                            );
#pragma warning restore 8509
                        }
                    }
                    else if (value != default)
                    {
                        // not JsonSerialize
                        entity[attribute.ColumnName] = ReflectionUtils.GetEdmCompatibleValue(value);
                    }
                    
                }
            }

            return entity;
        }


        /// <summary>
        /// Discover an object's metadata using reflection
        /// </summary>
        /// <typeparam name="TObject">Type of business object</typeparam>
        /// <param name="minimalSurface">If true, only loads the PartitionKey, RowKey and ETag properties.</param>
        /// <returns>Type metadata</returns>
        /// <exception cref="TypeLoadException">Exceptions are thrown if object is missing key attributes or values</exception>
        public static TypeMetadata Discover<TObject>(bool minimalSurface = false)
            where TObject : class
        {
            Type classType = typeof(TObject);
            string cacheKeyName = classType.FullName ?? classType.Name;
            TypeMetadata? meta = ReflectionCache.TryGet(cacheKeyName);
            if (meta != default)
            {
                return meta;
            }

            TableAttribute? tableAttribute = classType.GetCustomAttribute<TableAttribute>(true);
            if ((tableAttribute == default) || string.IsNullOrWhiteSpace(tableAttribute.TableName))
            {
                throw new TypeLoadException($"The type '{typeof(TObject).Name}' does not have a [Table] attribute.");
            }

            meta = new()
            {
                TableName = tableAttribute.TableName,
                UseSoftDelete = tableAttribute.UseSoftDelete
            };

            foreach (MemberInfo member in classType.GetMembers(MEMBER_SEARCH_FLAGS))
            {
                object[] memberAttributes = member.GetCustomAttributes(true);
                if ((memberAttributes == null) || (memberAttributes.Length == 0))
                {
                    continue;
                }

                foreach (object attribute in memberAttributes)
                {
                    if (attribute is PartitionKeyAttribute)
                    {
                        if (meta.PartitionKey != default)
                        {
                            throw new TypeLoadException($"'{cacheKeyName}' has multiple 'PartitionKey' properties defined.");
                        }
                        meta.PartitionKey = member;
                    }

                    if (attribute is RowKeyAttribute)
                    {
                        if (meta.RowKey != default)
                        {
                            throw new TypeLoadException($"'{cacheKeyName}' has multiple 'RowKey' properties defined.");
                        }
                        meta.RowKey = member;
                    }

                    if (attribute is ETagAttribute)
                    {
                        if (meta.ETag != default)
                        {
                            throw new TypeLoadException($"'{cacheKeyName}' has multiple 'ETag' properties defined.");
                        }
                        meta.ETag = member;
                    }

                    if ((!minimalSurface) && (attribute is TableColumnAttribute))
                    {
                        meta.Members.Add(member);
                    }
                }

                if (minimalSurface && (meta.PartitionKey != default) && (meta.RowKey != default))
                {
                    break;
                }
            }

            return meta;
        }

        private static readonly string[] RESERVED_MEMBER_NAMES = new[] { "PartitionKey", "RowKey", "ETag" };
        private static readonly BindingFlags MEMBER_SEARCH_FLAGS = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static;

        public const string ISDELETED_COLUMN_NAME = "IsDeleted";
    }



}
