﻿using System.Collections.Generic;

namespace SujaySarma.Data.Azure.Tables.Reflection
{
    /// <summary>
    /// A classes inspected and cached by the Type inspection engine
    /// </summary>
    internal static class ReflectionCache
    {
        /// <summary>
        /// Add an item to cache if it is not already in it
        /// </summary>
        /// <param name="info">Item to add</param>
        /// <param name="keyName">The key name of the object</param>
        public static void TrySet(TypeMetadata info, string keyName)
        {
            lock (_lock)
            {
                if (!_cache.ContainsKey(keyName))
                {
                    _cache.Add(keyName, info);
                }
            }
        }

        /// <summary>
        /// Fetch an item from cache
        /// </summary>
        /// <param name="keyName">Key name of object</param>
        /// <returns>Cached information or NULL</returns>
        public static TypeMetadata? TryGet(string keyName)
        {
            if (!_cache.TryGetValue(keyName, out TypeMetadata? info))
            {
                return default;
            }

            return info;
        }

        private static readonly Dictionary<string, TypeMetadata> _cache = new();
        private static readonly object _lock = new();
    }
}
