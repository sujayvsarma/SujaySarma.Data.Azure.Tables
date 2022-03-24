using SujaySarma.Data.Azure.Tables.Internal.Helpers;

using System;

namespace SujaySarma.Data.Azure.Tables.Internal.Reflection
{
    public static class ReflectionUtils
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

            if (Edm.NeedsConversion(convertFromType, convertToType))
            {
                return Edm.ConvertTo(convertToType, value);
            }

            return value;
        }

    }
}
