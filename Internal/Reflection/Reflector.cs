using SujaySarma.Data.Azure.Tables.Attributes;

using System.Reflection;

namespace Internal.Reflection
{
    internal class Reflector
    {
        /// <summary>
        /// Inspect a class for Azure Tables
        /// </summary>
        /// <typeparam name="ClassT">Type of business class to be stored into Azure Tables</typeparam>
        /// <returns>Reflected class metadata</returns>
        public static Class? InspectForAzureTables<ClassT>()
            where ClassT : class
        => InspectForAzureTables(typeof(ClassT));


        /// <summary>
        /// Inspect a class for Azure Tables
        /// </summary>
        /// <param name="classType">Type of business class to be stored into Azure Tables</param>
        /// <returns>Reflected class metadata</returns>
        public static Class? InspectForAzureTables(Type classType)
        {
            string cacheKeyName = classType.FullName ?? classType.Name;
            Class? objectMetadata = ReflectionCache.TryGet(cacheKeyName);
            if (objectMetadata != null)
            {
                // cache hit
                return objectMetadata;
            }

            TableAttribute? tableAttribute = classType.GetCustomAttribute<TableAttribute>(true);
            if (tableAttribute == null)
            {
                return null;
            }

            List<MemberProperty> properties = new();
            List<MemberField> fields = new();

            bool hasPartitionKey = false, hasRowKey = false, hasETag = false, hasTimestamp = false;

            foreach (MemberInfo member in classType.GetMembers(MEMBER_SEARCH_FLAGS))
            {
                object[] memberAttributes = member.GetCustomAttributes(true);
                if ((memberAttributes == null) || (memberAttributes.Length == 0))
                {
                    continue;
                }

                foreach (object attribute in memberAttributes)
                {
                    if (attribute is ETagAttribute)
                    {
                        if (hasETag)
                        {
                            throw new InvalidOperationException($"'{cacheKeyName}' has multiple ETag properties defined.");
                        }

                        hasETag = true;
                    }

                    if (attribute is PartitionKeyAttribute)
                    {
                        if (hasPartitionKey)
                        {
                            throw new InvalidOperationException($"'{cacheKeyName}' has multiple PartitionKey properties defined.");
                        }

                        hasPartitionKey = true;
                    }

                    if (attribute is RowKeyAttribute)
                    {
                        if (hasRowKey)
                        {
                            throw new InvalidOperationException($"'{cacheKeyName}' has multiple RowKey properties defined.");
                        }

                        hasRowKey = true;
                    }

                    if ((!hasTimestamp) && (attribute is TimestampAttribute))
                    {
                        // since we never read this back into the TableEntity, there can be as many of these as the developer wants :)
                        hasTimestamp = true;
                    }
                }

                if (!(hasPartitionKey || hasRowKey))
                {
                    continue;
                }

                switch (member.MemberType)
                {
                    case MemberTypes.Field:
                        FieldInfo? fi = member as FieldInfo;
                        if (fi != null)
                        {
                            fields.Add(new MemberField(fi));
                        }
                        break;

                    case MemberTypes.Property:
                        PropertyInfo? pi = member as PropertyInfo;
                        if (pi != null)
                        {
                            properties.Add(new MemberProperty(pi));
                        }
                        break;
                }
            }

            if ((properties.Count == 0) && (fields.Count == 0) || (!hasPartitionKey) || (!hasRowKey))
            {
                return null;
            }

            objectMetadata = new Class(classType.Name, tableAttribute, properties, fields)
            {
                // without these two, we don't get here!
                HasPartitionKey = true,
                HasRowKey = true,

                HasETag = hasETag,
                HasTimestamp = hasTimestamp
            };

            ReflectionCache.TrySet(objectMetadata, cacheKeyName);
            return objectMetadata;
        }

        private static readonly BindingFlags MEMBER_SEARCH_FLAGS = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static;
    }
}
