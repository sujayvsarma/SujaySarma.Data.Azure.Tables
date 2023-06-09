﻿using System.ComponentModel;
using System.Reflection;
using System;

namespace SujaySarma.Data.Azure.Tables.Internal.Helpers
{
    /// <summary>
    /// Converts data between Edm (Azure Table) types to .Net types
    /// </summary>
    internal static class Edm
    {

        /// <summary>
        /// Returns if the value needs to be converted between the two types
        /// </summary>
        /// <param name="edmType">The data type in Azure Table</param>
        /// <param name="clrType">The .NET type</param>
        /// <returns>True if type needs to be converted</returns>
        public static bool NeedsConversion(Type edmType, Type clrType)
        {
            if (!IsEdmCompatibleType(clrType))
            {
                throw new TypeLoadException($"'{clrType.Name}' is not compatible for Edm.");
            }

            return (!edmType.FullName!.Equals(clrType.FullName, StringComparison.Ordinal));
        }

        /// <summary>
        /// Convert between types
        /// </summary>
        /// <param name="destinationType">CLR Type of destination</param>
        /// <param name="value">The value to convert</param>
        /// <returns>The converted value</returns>
        public static object? ConvertTo(Type destinationType, object value)
        {
            //NOTE: value is not null -- already been checked by caller before calling here

            if (destinationType.IsEnum && (value is string val))
            {
                // Input is a string, destination is an Enum, Enum.Parse() it to convert!
                // We are using Parse() and not TryParse() with good reason. Bad values will throw exceptions to the top-level caller 
                // and we WANT that to happen! -- not only that, TryParse requires an extra typed storage that we do not want to provide here!

                return Enum.Parse(destinationType, val);
            }

            TypeConverter converter = TypeDescriptor.GetConverter(destinationType);
            if ((converter != null) && (converter.CanConvertTo(destinationType)))
            {
                return converter.ConvertTo(value, destinationType);
            }

            // see if type has a Parse static method
            MethodInfo[] methods = destinationType.GetMethods(BindingFlags.Public | BindingFlags.Static);
            if ((methods != null) && (methods.Length > 0))
            {
                Type sourceType = ((value == null) ? typeof(object) : value.GetType());
                foreach (MethodInfo m in methods)
                {
                    if (m.Name.Equals("Parse"))
                    {
                        ParameterInfo? p = m.GetParameters()?[0];
                        if ((p != null) && (p.ParameterType == sourceType))
                        {
                            return m.Invoke(null, new object?[] { value });
                        }
                    }
                    else if (m.Name.Equals("TryParse"))
                    {
                        ParameterInfo? p = m.GetParameters()?[0];
                        if ((p != null) && (p.ParameterType == sourceType))
                        {
                            object?[]? parameters = new object?[] { value, null };
                            bool? tpResult = (bool?)m.Invoke(null, parameters);
                            return ((tpResult.HasValue && tpResult.Value) ? parameters[1] : default);
                        }
                    }
                }
            }

            throw new TypeLoadException($"Could not find type converters for '{destinationType.Name}' type.");
        }

        /// <summary>
        /// Checks if the provided type is compatible with Edm data types
        /// </summary>
        /// <param name="clrType">The .NET CLR type to check</param>
        /// <returns>True if compatible.</returns>
        public static bool IsEdmCompatibleType(Type clrType)
            => (
                       (clrType.IsEnum)

                    // Non-NULL types

                    || (clrType == typeof(string))
                    || (clrType == typeof(byte[]))
                    || (clrType == typeof(bool))
                    || (clrType == typeof(DateTime))
                    || (clrType == typeof(DateTimeOffset))
                    || (clrType == typeof(double))
                    || (clrType == typeof(Guid))
                    || (clrType == typeof(int)) || (clrType == typeof(uint))
                    || (clrType == typeof(long)) || (clrType == typeof(ulong))
                    
                    // Nullable equivalents

                    || (clrType == typeof(byte?[]))
                    || (clrType == typeof(bool?))
                    || (clrType == typeof(DateTime?))
                    || (clrType == typeof(DateTimeOffset?))
                    || (clrType == typeof(double?))
                    || (clrType == typeof(Guid?))
                    || (clrType == typeof(int?)) || (clrType == typeof(uint?))
                    || (clrType == typeof(long?)) || (clrType == typeof(ulong?))
                );

    }
}
