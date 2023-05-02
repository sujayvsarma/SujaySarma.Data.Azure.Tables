using System;
using System.ComponentModel;
using System.Reflection;

namespace SujaySarma.Data.Azure.Tables.Reflection
{
    /// <summary>
    /// Reflection utilities    
    /// </summary>
    internal static class ReflectionUtils
    {

        /// <summary>
        /// Returns a value that matches the destination type
        /// </summary>
        /// <param name="sourceType">Type of value being provided</param>
        /// <param name="destinationType">Type of the destination container</param>
        /// <param name="value">Value to convert/change</param>
        /// <returns>The value of type destinationType</returns>
        public static object? GetAcceptableValue(Type sourceType, Type destinationType, object? value)
        {
            Type? srcActualType = Nullable.GetUnderlyingType(sourceType);
            Type convertFromType = srcActualType ?? sourceType;

            Type? destActualType = Nullable.GetUnderlyingType(destinationType);
            Type convertToType = destActualType ?? destinationType;

            if (value == null)
            {
                return null;
            }

            if (NeedsConversion(convertFromType, convertToType))
            {
                return ConvertTo(convertToType, value);
            }

            return value;
        }


        /// <summary>
        /// Returns a value matching the destination type
        /// </summary>
        /// <typeparam name="TResult">Type of destination</typeparam>
        /// <param name="value">Value to convert</param>
        /// <returns>The value of type <typeparamref name="TResult"/></returns>
        public static TResult? GetAcceptableValue<TResult>(object? value)
            => (TResult?)GetAcceptableValue(value?.GetType() ?? typeof(object), typeof(TResult), value);

        /// <summary>
        /// Get a value from a property or field with the correct type
        /// </summary>
        /// <typeparam name="TObjectInstance">Type of business object with the property or field</typeparam>
        /// <typeparam name="TValue">Type of value to return</typeparam>
        /// <param name="instance">Instance of the business object</param>
        /// <param name="propertyOrField">Reflection metadata abotu the property or field whose value is to be read</param>
        /// <returns>Value coerced to the correct type or default</returns>
        public static TValue? GetValue<TObjectInstance, TValue>(TObjectInstance instance, MemberInfo propertyOrField)
        {
            object? value = default;
            Type sourceType = typeof(object);
            if (propertyOrField is FieldInfo field)
            {
                value = field.GetValue(instance);
                sourceType = field.FieldType;
            }
            else if (propertyOrField is PropertyInfo property)
            {
                value = property.GetValue(instance);
                sourceType = property.PropertyType;
            }

            return (TValue?)GetAcceptableValue(sourceType, typeof(TValue), value);
        }

        /// <summary>
        /// Get a value from a property or field with the correct type
        /// </summary>
        /// <typeparam name="TObjectInstance">Type of business object with the property or field</typeparam>
        /// <param name="instance">Instance of the business object</param>
        /// <param name="propertyOrField">Reflection metadata abotu the property or field whose value is to be read</param>
        /// <returns>Value coerced to the correct type or default</returns>
        public static object? GetValue<TObjectInstance>(TObjectInstance instance, MemberInfo propertyOrField)
        {
            if (propertyOrField is FieldInfo field)
            {
                return field.GetValue(instance);
            }

            PropertyInfo property = (PropertyInfo)propertyOrField;
            return property.GetValue(instance);
        }

        /// <summary>
        /// Get a value from a property or field that is compatible with the Table Storage's EDM datatype set.
        /// </summary>
        /// <param name="value">Value to "convert"</param>
        /// <returns>Edm compat values are returned directly. Others are converted to String.</returns>
        public static object? GetEdmCompatibleValue(object value)
        {
            Type sourceType = value.GetType();
            Type? srcActualType = Nullable.GetUnderlyingType(sourceType);
            Type convertFromType = srcActualType ?? sourceType;

            if (IsEdmCompatibleType(convertFromType))
            {
                if (value is DateTime dt)
                {
                    // AST requires dates to be UTC without this the operation will fail
                    value = DateTime.SpecifyKind(dt, DateTimeKind.Utc);
                }

                return value;
            }

            // value is not Edm compatible the simplest thing we can do 
            // is to pull a .ToString(). This works for Enums as well.
            return value.ToString();
        }

        /// <summary>
        /// Cooerce a value from EDM to a CLR type 
        /// </summary>
        /// <param name="value">Value from Edm</param>
        /// <param name="targetClrType">The target CLR type to convert to</param>
        /// <returns>Cooerced value</returns>
        public static object? CoerceFromEdmValue(object? value, Type targetClrType)
        {
            if (value == default)
            {
                return default;
            }

            Type sourceEdmType = value.GetType();
            Type? actualClrType = Nullable.GetUnderlyingType(targetClrType);
            Type destinationClrType = actualClrType ?? targetClrType;

            if (!sourceEdmType.FullName!.Equals(destinationClrType.FullName, StringComparison.Ordinal))
            {
                // we need to convert
                return ConvertTo(destinationClrType, value);
            }

            return value;
        }


        /// <summary>
        /// Set a value into a property or field with the correct type
        /// </summary>
        /// <typeparam name="TObjectInstance">Type of business object with the property or field</typeparam>
        /// <param name="instance">Instance of the business object</param>
        /// <param name="propertyOrField">Reflection metadata abotu the property or field whose value is to be written</param>
        /// <param name="value">The value to be written</param>
        public static void SetValueFromEdm<TObjectInstance>(TObjectInstance instance, MemberInfo propertyOrField, object? value)
        {
            if (propertyOrField is FieldInfo field)
            {
                value = CoerceFromEdmValue(value, field.FieldType);
                field.SetValue(instance, value);
            }
            else if (propertyOrField is PropertyInfo property)
            {
                value = CoerceFromEdmValue(value, property.PropertyType);
                property.SetValue(instance, value);

            }
        }


        /// <summary>
        /// Set a value into a property or field with the correct type
        /// </summary>
        /// <typeparam name="TObjectInstance">Type of business object with the property or field</typeparam>
        /// <param name="instance">Instance of the business object</param>
        /// <param name="propertyOrField">Reflection metadata abotu the property or field whose value is to be written</param>
        /// <param name="value">The value to be written</param>
        public static void SetValue<TObjectInstance>(TObjectInstance instance, MemberInfo propertyOrField, object? value)
        {
            if (propertyOrField is FieldInfo field)
            {
                value = GetAcceptableValue(value?.GetType() ?? typeof(object), field.FieldType, value);
                field.SetValue(instance, value);
            }
            else if (propertyOrField is PropertyInfo property)
            {
                value = GetAcceptableValue(value?.GetType() ?? typeof(object), property.PropertyType, value);
                property.SetValue(instance, value);                
            }
        }


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

            return !edmType.FullName!.Equals(clrType.FullName, StringComparison.Ordinal);
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

            // Adding support for new .NET types DateOnly and TimeOnly
            if (value is DateTimeOffset dto)
            {
                if (destinationType == typeof(DateTime))
                {
                    return dto.UtcDateTime;
                }

                if (destinationType == typeof(DateOnly))
                {
                    return new DateOnly(dto.Year, dto.Month, dto.Day);
                }

                if (destinationType == typeof(TimeOnly))
                {
                    return new TimeOnly(dto.Hour, dto.Minute, dto.Second);
                }

                return dto;
            }
            else if (value is DateTime dt)
            {
                if (destinationType == typeof(DateTimeOffset))
                {
                    return new DateTimeOffset(dt);
                }

                if (destinationType == typeof(DateOnly))
                {
                    return new DateOnly(dt.Year, dt.Month, dt.Day);
                }

                if (destinationType == typeof(TimeOnly))
                {
                    return new TimeOnly(dt.Hour, dt.Minute, dt.Second);
                }

                return dt;
            }

            TypeConverter converter = TypeDescriptor.GetConverter(destinationType);
            if ((converter != null) && converter.CanConvertTo(destinationType))
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
            =>
                       
                    // clrType.IsEnum ||    /*(Enum is not supported by the Azure.Data.Tables package!)*/

                    // Non-NULL types

                       clrType == typeof(string)
                    || clrType == typeof(byte[])
                    || clrType == typeof(bool)
                    || clrType == typeof(DateTime)
                    || clrType == typeof(DateTimeOffset)
                    || clrType == typeof(double)
                    || clrType == typeof(Guid)
                    || clrType == typeof(int) || clrType == typeof(uint)
                    || clrType == typeof(long) || clrType == typeof(ulong)

                    // Nullable equivalents

                    || clrType == typeof(byte?[])
                    || clrType == typeof(bool?)
                    || clrType == typeof(DateTime?)
                    || clrType == typeof(DateTimeOffset?)
                    || clrType == typeof(double?)
                    || clrType == typeof(Guid?)
                    || clrType == typeof(int?) || clrType == typeof(uint?)
                    || clrType == typeof(long?) || clrType == typeof(ulong?)
                ;


    }
}
