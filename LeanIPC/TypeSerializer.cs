using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace LeanIPC
{
    /// <summary>
    /// Describes how to treat a non-primitive type in serialization
    /// </summary>
    public enum SerializationAction
    {
        /// <summary>
        /// Creates a reference, passing a handle
        /// </summary>
        Reference,

        /// <summary>
        /// Splits the element into its fields, and transmits thos
        /// </summary>
        Decompose,

        /// <summary>
        /// Ignores the element if it is a field of a type that is serialized
        /// </summary>
        Ignore,

        /// <summary>
        /// Fails when attempting to serialize it
        /// </summary>
        Fail,

        /// <summary>
        /// Default is to decompose struct types and types and fail on others
        /// </summary>
        Default
    }

    /// <summary>
    /// Helper class that performs quick-ish serialization and deserialization of types and typenames.
    /// By using the naming scheme in this class, we can emit shorter messages as long as the transmitted types
    /// are any of the regularly used .Net types.
    /// </summary>
    public sealed class TypeSerializer
    {
        /// <summary>
        /// The default type serializer
        /// </summary>
        public static TypeSerializer Default = new TypeSerializer(false, false);

        /// <summary>
        /// Lookup table with short names
        /// </summary>
        private static readonly Dictionary<Type, string> _typeToShortName = new Dictionary<Type, string>();
        /// <summary>
        /// Lookup table with types
        /// </summary>
        private static readonly Dictionary<string, Type> _shortNameToType = new Dictionary<string, Type>();

        /// <summary>
        /// A cache for keeping type names, to avoid re-generating the strings
        /// </summary>
        private readonly Dictionary<Type, string> m_typeCache = new Dictionary<Type, string>();

        /// <summary>
        /// A cache for keeping types, to avoid repeated string parsing
        /// </summary>
        private readonly Dictionary<string, Type> m_nameCache = new Dictionary<string, Type>();

        /// <summary>
        /// A cache for keeping types, to avoid repeated string parsing
        /// </summary>
        private readonly Dictionary<string, Type[]> m_namesCache = new Dictionary<string, Type[]>();

        /// <summary>
        /// A cache for keeping member string representations
        /// </summary>
        private readonly Dictionary<System.Reflection.MemberInfo, string> m_memberItemCache = new Dictionary<System.Reflection.MemberInfo, string>();

        /// <summary>
        /// A cache for keeping member information from parsed strings
        /// </summary>
        private readonly Dictionary<string, System.Reflection.MemberInfo> m_memberStringCache = new Dictionary<string, System.Reflection.MemberInfo>();

        /// <summary>
        /// The custom typenames and their mapped type
        /// </summary>
        private readonly Dictionary<string, Type> m_typenameOverridesName = new Dictionary<string, Type>();
        /// <summary>
        /// The custom types to override names for
        /// </summary>
        private readonly Dictionary<Type, string> m_typenameOverridesType = new Dictionary<Type, string>();

        /// <summary>
        /// Hook for choosing custom serialization strategies for select types
        /// </summary>
        private readonly Dictionary<Type, SerializationAction> m_serializationOverrides = new Dictionary<Type, SerializationAction>();

        /// <summary>
        /// Returns custom serialization filters for select types
        /// </summary>
        private readonly Dictionary<Type, Func<System.Reflection.FieldInfo[], System.Reflection.FieldInfo[]>> m_serializationFilters = new Dictionary<Type, Func<System.Reflection.FieldInfo[], System.Reflection.FieldInfo[]>>();

        /// <summary>
        /// Allows for custom serialization
        /// </summary>
        private readonly Dictionary<Type, Func<Type, object, Tuple<Type[], object[]>>> m_customSerializer = new Dictionary<Type, Func<Type, object, Tuple<Type[], object[]>>>();

        /// <summary>
        /// Allows custom deserialization
        /// </summary>
        private readonly Dictionary<Type, Func<Type, object[], object>> m_customDeserializer = new Dictionary<Type, Func<Type, object[], object>>();

        /// <summary>
        /// A lock used to guard the shared data structures
        /// </summary>
        private readonly object m_lock = new object();

        /// <summary>
        /// A list of types that are considered primitive
        /// </summary>
        public static readonly Type[] PRIMITIVE_TYPES = {
            typeof(bool),
            typeof(char),
            typeof(sbyte),
            typeof(byte),
            typeof(ushort),
            typeof(short),
            typeof(uint),
            typeof(int),
            typeof(ulong),
            typeof(long),
            typeof(float),
            typeof(double),
            typeof(string),
            typeof(Type),
            typeof(DateTime),
            typeof(TimeSpan),
            typeof(Exception),
            typeof(void)
        };

        /// <summary>
        /// Class for keeping track of letter assingments
        /// </summary>
        private static class Letters
        {
            public const char BOOL = 'z';
            public const char CHAR = 'c';
            public const char SBYTE = 'h';
            public const char BYTE = 'b';
            public const char USHORT = 'u';
            public const char SHORT = 'o';
            public const char UINT = 'n';
            public const char INT = 'i';
            public const char ULONG = 'g';
            public const char LONG = 'l';
            public const char FLOAT = 'f';
            public const char DOUBLE = 'd';
            public const char STRING = 's';
            public const char TYPE = 't';
            public const char DATETIME = 'm';
            public const char TIMESPAN = 'p';
            public const char EXCEPTION = 'x';
            public const char VOID = 'v';

            public const char GENERIC = 'r';
            public const char ARRAY = 'a';
            public const char KEYVALUEPAIR = 'k';
            public const char LIST = 'w';
            public const char TUPLE = 'e';
            public const char DICTIONARY = 'y';
            public const char TASK = 'j';
            public const char TASKP = 'q';
        }

        /// <summary>
        /// Initialize the basic types used in all the type mapping code
        /// </summary>
        static TypeSerializer()
        {
            var chars =
                typeof(Letters)
                    .GetFields(System.Reflection.BindingFlags.Static | System.Reflection.BindingFlags.Public)
                    .Where(x => x.FieldType == typeof(char))
                    .Select(x => x.GetValue(null))
                    .Cast<char>()
                    .ToArray();

            if (chars.Length != chars.Distinct().Count())
            {
                foreach (var c in chars)
                    if (chars.Count(x => x == c) > 1)
                        throw new Exception($"Duplicate letter assignment for {c}!");

                throw new Exception($"Duplicate letter assignment!");
            }

            _typeToShortName[typeof(bool)] = Letters.BOOL.ToString();
            _typeToShortName[typeof(char)] = Letters.CHAR.ToString();
            _typeToShortName[typeof(sbyte)] = Letters.SBYTE.ToString();
            _typeToShortName[typeof(byte)] = Letters.BYTE.ToString();
            _typeToShortName[typeof(ushort)] = Letters.USHORT.ToString();
            _typeToShortName[typeof(short)] = Letters.SHORT.ToString();
            _typeToShortName[typeof(uint)] = Letters.UINT.ToString();
            _typeToShortName[typeof(int)] = Letters.INT.ToString();
            _typeToShortName[typeof(ulong)] = Letters.ULONG.ToString();
            _typeToShortName[typeof(long)] = Letters.LONG.ToString();
            _typeToShortName[typeof(float)] = Letters.FLOAT.ToString();
            _typeToShortName[typeof(double)] = Letters.DOUBLE.ToString();
            _typeToShortName[typeof(string)] = Letters.STRING.ToString();
            _typeToShortName[typeof(Type)] = Letters.TYPE.ToString();
            _typeToShortName[typeof(DateTime)] = Letters.DATETIME.ToString();
            _typeToShortName[typeof(TimeSpan)] = Letters.TIMESPAN.ToString();
            _typeToShortName[typeof(Exception)] = Letters.EXCEPTION.ToString();
            _typeToShortName[typeof(void)] = Letters.VOID.ToString();
            _typeToShortName[typeof(Task)] = Letters.TASK.ToString();

            foreach (var n in _typeToShortName)
                _shortNameToType[n.Value] = n.Key;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.TypeSerializer"/> class.
        /// </summary>
        public TypeSerializer()
            : this(true)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.TypeSerializer"/> class.
        /// </summary>
        /// <param name="copydefaults">If set to <c>true</c> copy settings from the default instance.</param>
        /// <param name="copycache">Copies the cache contents if <paramref name="copydefaults"/> is also set to <c>true</c></param>
        public TypeSerializer(bool copydefaults, bool copycache = true)
        {
            if (copydefaults)
            {
                foreach (var n in Default.m_typenameOverridesName)
                    m_typenameOverridesName.Add(n.Key, n.Value);
                foreach (var n in Default.m_typenameOverridesType)
                    m_typenameOverridesType.Add(n.Key, n.Value);

                foreach (var n in Default.m_customSerializer)
                    m_customSerializer.Add(n.Key, n.Value);
                foreach (var n in Default.m_customDeserializer)
                    m_customDeserializer.Add(n.Key, n.Value);

                foreach (var n in Default.m_serializationOverrides)
                    m_serializationOverrides.Add(n.Key, n.Value);
                foreach (var n in Default.m_serializationFilters)
                    m_serializationFilters.Add(n.Key, n.Value);

                if (copycache)
                {
                    foreach (var n in Default.m_nameCache)
                        m_nameCache.Add(n.Key, n.Value);
                    foreach (var n in Default.m_typeCache)
                        m_typeCache.Add(n.Key, n.Value);
                    foreach (var n in Default.m_namesCache)
                        m_namesCache.Add(n.Key, n.Value);
                    foreach (var n in Default.m_memberItemCache)
                        m_memberItemCache.Add(n.Key, n.Value);
                    foreach (var n in Default.m_memberStringCache)
                        m_memberStringCache.Add(n.Key, n.Value);
                }
            }
        }

        /// <summary>
        /// Clears the local cache
        /// </summary>
        public void ClearCache()
        {
            lock (m_lock)
            {
                m_nameCache.Clear();
                m_typeCache.Clear();
                m_namesCache.Clear();
                m_memberItemCache.Clear();
                m_memberStringCache.Clear();
            }
        }

        /// <summary>
        /// Registers a custom name for a type, such that the full typename does not need to be submitted
        /// </summary>
        /// <param name="type">The type to register.</param>
        /// <param name="name">The name of the type to register.</param>
        public void RegisterTypename(Type type, string name)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("Name cannot be empty", nameof(name));
            if (name.IndexOfAny(new[] { '{', '}' }) >= 0)
                throw new ArgumentException("Name cannot contain { or }", nameof(name));
            if (m_typenameOverridesName.ContainsKey(name))
                throw new ArgumentException("Name is already registered", nameof(name));
            if (m_typenameOverridesType.ContainsKey(type))
                throw new ArgumentException("Type is already registered", nameof(type));

            lock (m_lock)
            {
                m_typenameOverridesName.Add(name, type);
                m_typenameOverridesType.Add(type, name);
                ClearCache();
            }
        }

        /// <summary>
        /// Unregister a custom name for a type
        /// </summary>
        /// <returns><c>true</c>, if typename was unregistered, <c>false</c> otherwise.</returns>
        /// <param name="type">The type to unregister.</param>
        public bool UnregisterTypename(Type type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            lock(m_lock)
                if (m_typenameOverridesType.TryGetValue(type, out var name))
                {
                    m_typenameOverridesType.Remove(type);
                    m_typenameOverridesName.Remove(name);
                    ClearCache();
                    return true;
                }
            return false;
        }

        /// <summary>
        /// Unregister a custom name for a type
        /// </summary>
        /// <returns><c>true</c>, if typename was unregistered, <c>false</c> otherwise.</returns>
        /// <param name="name">The name to unregister.</param>
        public bool UnregisterTypename(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
                throw new ArgumentException("The name cannot be empty", nameof(name));

            lock(m_lock)
                if (m_typenameOverridesName.TryGetValue(name, out var type))
                {
                    m_typenameOverridesType.Remove(type);
                    m_typenameOverridesName.Remove(name);
                    ClearCache();
                    return true;
                }
            return false;
        }

        /// <summary>
        /// Registers a specific serialization action for a type
        /// </summary>
        /// <param name="type">The type to register the action for.</param>
        /// <param name="action">The action to use when serializing the item.</param>
        public void RegisterSerializationAction(Type type, SerializationAction action)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            lock (m_lock)
            {
                m_serializationOverrides.Add(type, action);
                ClearCache();
            }
        }

        /// <summary>
        /// Unregisters a specific serialization action for a type
        /// </summary>
        /// <param name="type">The type to unregister the action for.</param>
        public bool UnregisterSerializationAction(Type type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            bool res;
            lock (m_lock)
            {
                res = m_serializationOverrides.Remove(type);
                if (res)
                    ClearCache();
            }
            return res;
        }

        /// <summary>
        /// Registers a custom serialization filter
        /// </summary>
        /// <param name="type">The type to filter fields for.</param>
        /// <param name="handler">The handler that performs the filtering.</param>
        public void RegisterSerializationFilter(Type type, Func<System.Reflection.FieldInfo[], System.Reflection.FieldInfo[]> handler)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));
            if (handler == null)
                throw new ArgumentNullException(nameof(handler));

            lock (m_lock)
            {
                m_serializationFilters.Add(type, handler);
                ClearCache();
            }
        }

        /// <summary>
        /// Unregisters a custom serialization filter.
        /// </summary>
        /// <returns><c>true</c>, if serialization filter was unregistered, <c>false</c> otherwise.</returns>
        /// <param name="type">The type to remove the filter for.</param>
        public bool UnregisterSerializationFilter(Type type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            bool res;
            lock (m_lock)
            {
                res = m_serializationFilters.Remove(type);
                if (res)
                    ClearCache();
            }
            return res;
        }

        /// <summary>
        /// Registers a custom serializer and deserializer.
        /// </summary>
        /// <param name="type">The type to register the custom serializer for.</param>
        /// <param name="serializer">The serializer function to use.</param>
        /// <param name="deserializer">The deserializer function to use.</param>
        public void RegisterCustomSerializer(Type type, Func<Type, object, Tuple<Type[], object[]>> serializer, Func<Type, object[], object> deserializer)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));
            if (serializer == null)
                throw new ArgumentNullException(nameof(serializer));
            if (deserializer == null)
                throw new ArgumentNullException(nameof(deserializer));

            lock (m_lock)
            {
                m_customSerializer.Add(type, serializer);
                m_customDeserializer.Add(type, deserializer);
                ClearCache();
            }
        }

        /// <summary>
        /// Unregisters a custom serializer.
        /// </summary>
        /// <param name="type">The type to unregister the serializer for.</param>
        public bool UnregisterCustomSerializer(Type type)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            bool res;
            lock (m_lock)
            {
                res = m_customSerializer.Remove(type);
                res |= m_customDeserializer.Remove(type);
                if (res)
                    ClearCache();
            }
            return res;
        }

        /// <summary>
        /// Get a short string representation of the given type
        /// </summary>
        /// <returns>The short typename.</returns>
        /// <param name="t">The type to get the name for</param>
        public string GetShortTypeName(Type t)
        {
            if (t == null)
                throw new ArgumentNullException(nameof(t));
            if (_typeToShortName.TryGetValue(t, out var s))
                return s;
            
            lock(m_lock)
                if (m_typeCache.TryGetValue(t, out s))
                    return s;

            if (t.IsGenericType)
            {
                var gt = t.GetGenericTypeDefinition();
                var ga = string.Join("", t.GetGenericArguments().Select(GetShortTypeName));
                if (gt == typeof(KeyValuePair<,>))
                {
                    lock(m_lock)
                        return m_typeCache[t] = Letters.KEYVALUEPAIR + "{" + ga + "}";
                }
                else if (
                    gt == typeof(Tuple<>) ||
                    gt == typeof(Tuple<,>) ||
                    gt == typeof(Tuple<,,>) ||
                    gt == typeof(Tuple<,,,>) ||
                    gt == typeof(Tuple<,,,,>) ||
                    gt == typeof(Tuple<,,,,,>) ||
                    gt == typeof(Tuple<,,,,,,>) ||
                    gt == typeof(Tuple<,,,,,,,>))
                {
                    lock(m_lock)
                        return m_typeCache[t] = Letters.TUPLE + "{" + ga + "}";
                }
                if (gt == typeof(List<>))
                {
                    lock(m_lock)
                        return m_typeCache[t] = Letters.LIST + "{" + ga + "}";
                }
                else if (gt == typeof(Dictionary<,>))
                {
                    lock(m_lock)
                        return m_typeCache[t] = Letters.DICTIONARY + "{" + ga + "}";
                }
                else if (gt == typeof(Task<>))
                {
                    lock(m_lock)
                        return m_typeCache[t] = Letters.TASKP + "{" + ga + "}";
                }
                else
                {
                    lock(m_lock)
                        return m_typeCache[t] = Letters.GENERIC + "{" + t.FullName + "{" + ga + "}}";
                }
            }
            if (t.IsArray)
            {
                lock(m_lock)
                    return m_typeCache[t] = Letters.ARRAY + "{" + GetShortTypeName(t.GetElementType()) + "}";
            }

            if (m_typenameOverridesType.TryGetValue(t, out var name))
                lock(m_lock)
                    return m_typeCache[t] = "{" + name + "}";

            if (t.Assembly == System.Reflection.Assembly.GetExecutingAssembly())
                lock(m_lock)
                    return m_typeCache[t] = "{" + t.FullName + "}";

            // We need at least the assembly for the external types
            lock(m_lock)
                return m_typeCache[t] = "{" + t.FullName + "," + t.Assembly.GetName().Name + "}";
        }

        /// <summary>
        /// Returns a compact typedefinition string from a set of arguments
        /// </summary>
        /// <returns>The short typedefinition.</returns>
        /// <param name="types">The types to create the definition for.</param>
        public string GetShortTypeDefinition(params Type[] types)
        {
            return string.Join("", types.Select(GetShortTypeName));
        }

        /// <summary>
        /// Parses a short typename 
        /// </summary>
        /// <returns>The short type name.</returns>
        /// <param name="s">S.</param>
        public Type ParseShortTypeName(string s)
        {
            if (string.IsNullOrEmpty(s))
                throw new ArgumentNullException(nameof(s));
            if (_shortNameToType.TryGetValue(s, out var t))
                return t;
            lock(m_lock)
                if (m_nameCache.TryGetValue(s, out t))
                    return t;

            if (s[0] == '{')
            {
                var name = s.Substring(1, s.Length - 2);
                if (m_typenameOverridesName.TryGetValue(name, out var type))
                    lock(m_lock)
                        return m_nameCache[s] = type;
                
                lock(m_lock)
                    return m_nameCache[s] = Type.GetType(name, true);
            }
            else if (s[0] == Letters.ARRAY)
            {
                lock(m_lock)
                    return m_nameCache[s] = ParseShortTypeName(s.Substring(2, s.Length - 3)).MakeArrayType();
            }
            else if (s[0] == Letters.LIST)
            {
                var args = ParseShortTypeDefinition(s.Substring(2, s.Length - 3));
                lock(m_lock)
                    return m_nameCache[s] = typeof(List<>).MakeGenericType(args);
            }
            else if (s[0] == Letters.DICTIONARY)
            {
                var args = ParseShortTypeDefinition(s.Substring(2, s.Length - 3));
                lock(m_lock)
                    return m_nameCache[s] = typeof(Dictionary<,>).MakeGenericType(args);
            }
            else if (s[0] == Letters.TASKP)
            {
                var args = ParseShortTypeDefinition(s.Substring(2, s.Length - 3));
                lock(m_lock)
                    return m_nameCache[s] = typeof(Task<>).MakeGenericType(args);
            }
            else if (s[0] == Letters.GENERIC)
            {
                var ix = s.IndexOf('{', 2);
                var name = s.Substring(2, ix - 3);
                var gtype = Type.GetType(name, true);
                var args = ParseShortTypeDefinition(s.Substring(ix, s.Length - ix - 2));
                lock(m_lock)
                    return m_nameCache[s] = gtype.MakeGenericType(args);
            }
            else if (s[0] == Letters.TUPLE)
            {
                var args = ParseShortTypeDefinition(s.Substring(2, s.Length - 3));
                lock(m_lock)
                    switch (args.Length)
                    {
                        case 1: return m_nameCache[s] = typeof(Tuple<>).MakeGenericType(args);
                        case 2: return m_nameCache[s] = typeof(Tuple<,>).MakeGenericType(args);
                        case 3: return m_nameCache[s] = typeof(Tuple<,,>).MakeGenericType(args);
                        case 4: return m_nameCache[s] = typeof(Tuple<,,,>).MakeGenericType(args);
                        case 5: return m_nameCache[s] = typeof(Tuple<,,,,>).MakeGenericType(args);
                        case 6: return m_nameCache[s] = typeof(Tuple<,,,,,>).MakeGenericType(args);
                        case 7: return m_nameCache[s] = typeof(Tuple<,,,,,,>).MakeGenericType(args);
                        case 8: return m_nameCache[s] = typeof(Tuple<,,,,,,,>).MakeGenericType(args);
                        default:
                            throw new ArgumentException($"Failed to parse short typename for tuple with {args.Length} elements");
                    }
            }
            else if (s[0] == Letters.KEYVALUEPAIR)
            {
                var args = ParseShortTypeDefinition(s.Substring(2, s.Length - 3));
                lock(m_lock)
                    return m_nameCache[s] = typeof(KeyValuePair<,>).MakeGenericType(args);
            }
            else
                throw new ArgumentException("Failed to parse short typename");
        }

        /// <summary>
        /// Find the next matching brace in the string, assuming <paramref name="offset"/> points to the first
        /// </summary>
        /// <returns>The ending brace pair.</returns>
        /// <param name="s">The string to search.</param>
        /// <param name="offset">The search offset.</param>
        /// <param name="length">The maximum search length.</param>
        private static int FindEndingBraceIndex(string s, int offset, int length)
        {
            while (offset < length)
            {
                var end = s.IndexOf('}', offset + 1, length - offset - 1);
                if (end < 0)
                    throw new ArgumentException("Unmatched brace");

                //Check if there is a newly started brace inside
                var next = s.IndexOf('{', offset + 1, end - offset - 1);
                if (next < 0)
                    return end;

                offset = end;
            }

            throw new Exception("Unmatched brace");
        }

        /// <summary>
        /// Parses the short type definition.
        /// </summary>
        /// <returns>The short type definition.</returns>
        /// <param name="s">The string to parse.</param>
        public Type[] ParseShortTypeDefinition(string s)
        {
            if (s == null)
                throw new ArgumentNullException(nameof(s));
            if (s == string.Empty)
                return new Type[0];

            if (_shortNameToType.TryGetValue(s, out var t))
                return new Type[] { t };
            lock(m_lock)
                if (m_namesCache.TryGetValue(s, out var tr))
                    return tr;

            var types = new List<Type>();

            var ix = 0;
            while (ix < s.Length)
            {
                var composite =
                    s[ix] == '{' ||
                    s[ix] == Letters.KEYVALUEPAIR ||
                    s[ix] == Letters.DICTIONARY ||
                    s[ix] == Letters.GENERIC ||
                    s[ix] == Letters.LIST ||
                    s[ix] == Letters.TUPLE ||
                    s[ix] == Letters.TASKP ||
                    s[ix] == Letters.ARRAY;

                if (composite)
                {
                    var end = FindEndingBraceIndex(s, ix + (s[ix] == '{' ? 0 : 1), s.Length);
                    types.Add(ParseShortTypeName(s.Substring(ix, end - ix + 1)));
                    ix = end + 1;
                }
                else if (_shortNameToType.TryGetValue(s.Substring(ix, 1), out t))
                {
                    types.Add(t);
                    ix++;
                }
                else
                    throw new Exception($"Failed to parse short type definition: {s}");
            }

            lock(m_lock)
                return m_namesCache[s] = types.ToArray();
        }

        /// <summary>
        /// Gets a short string definition from a reflection method or property
        /// </summary>
        /// <returns>The short definition.</returns>
        /// <param name="method">The reflection method or property.</param>
        public string GetShortDefinition(System.Reflection.MemberInfo method)
        {
            if (method == null)
                throw new ArgumentNullException(nameof(method));

            lock(m_lock)
                if (m_memberItemCache.TryGetValue(method, out var s))
                    return s;

            if (method is System.Reflection.MethodInfo)
            {
                var mi = method as System.Reflection.MethodInfo;
                lock(m_lock)
                    return m_memberItemCache[method] = "M{" + method.Name + "{" + GetShortTypeName(mi.ReturnType) + "" + GetShortTypeDefinition(mi.GetParameters().Select(x => x.ParameterType).ToArray()) + "}}}";
            }
            if (method is System.Reflection.ConstructorInfo)
            {
                var ci = method as System.Reflection.ConstructorInfo;
                lock(m_lock)
                    return m_memberItemCache[method] = "C{" + method.Name + "{" + GetShortTypeDefinition(ci.GetParameters().Select(x => x.ParameterType).ToArray()) + "}}";
            }
            else if (method is System.Reflection.FieldInfo)
            {
                var fi = method as System.Reflection.FieldInfo;
                lock(m_lock)
                    return m_memberItemCache[method] = "F{" + method.Name + "{" + GetShortTypeName(fi.FieldType) + "}}";
            }
            else if (method is System.Reflection.PropertyInfo)
            {
                var pi = method as System.Reflection.PropertyInfo;
                var ixp = pi.GetIndexParameters();
                if (ixp.Length > 0)
                {
                    lock(m_lock)
                        return m_memberItemCache[method] = "I{" + pi.Name + "{" + GetShortTypeName(pi.PropertyType) + "" + GetShortTypeDefinition(ixp.Select(x => x.ParameterType).ToArray()) + "}}";
                }
                else
                {
                    lock(m_lock)
                        return m_memberItemCache[method] = "P{" + pi.Name + "{" + GetShortTypeName(pi.PropertyType) + "}}";
                }
            }

            throw new Exception($"Unable to create definition for {method.GetType().FullName}");
        }

        /// <summary>
        /// Parses a method or property description and returns the representation for it
        /// </summary>
        /// <returns>The from short definition.</returns>
        /// <param name="source">The source type.</param>
        /// <param name="s">The string to parse.</param>
        public System.Reflection.MemberInfo GetFromShortDefinition(Type source, string s)
        {
            if (string.IsNullOrWhiteSpace(s))
                throw new ArgumentNullException(nameof(s));

            var key = source.FullName + ":" + s;
            lock(m_lock)
                if (m_memberStringCache.TryGetValue(key, out var m))
                    return m;

            var t = s[0];
            var nix = s.IndexOf('{', 2);
            var name = s.Substring(2, nix - 2);
            var eb = FindEndingBraceIndex(s, nix, s.Length);
            var typeargs = ParseShortTypeDefinition(s.Substring(nix + 1, eb - nix - 1));

            if (s[0] == 'M')
            {
                var tmp = source.GetMethod(name, System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Static, null, typeargs.Skip(1).ToArray(), null);
                if (tmp == null)
                    throw new ArgumentException($"Failed to find method {name} on {source.FullName}");
                lock(m_lock)
                    return m_memberStringCache[key] = tmp;
            }
            else if (s[0] == 'C')
            {
                var tmp = source.GetConstructor(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance, null, typeargs, null);
                if (tmp == null)
                    throw new ArgumentException($"Failed to find constructor {name} on {source.FullName}");
                lock(m_lock)
                    return m_memberStringCache[key] = tmp;
            }
            else if (s[0] == 'I')
            {
                var tmp = source.GetProperty(name, System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Static, null, typeargs[0], typeargs.Skip(1).ToArray(), null);
                if (tmp == null)
                    throw new ArgumentException($"Failed to find indexed property {name} on {source.FullName}");
                lock(m_lock)
                    return m_memberStringCache[key] = tmp;
            }
            else if (s[0] == 'P')
            {
                var tmp = source.GetProperty(name, System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Static, null, typeargs[0], new Type[0], null);
                if (tmp == null)
                    throw new ArgumentException($"Failed to find property {name} on {source.FullName}");
                lock (m_lock)
                    return m_memberStringCache[key] = tmp;
            }
            else if (s[0] == 'F')
            {
                var tmp = source.GetField(name, System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Static);
                if (tmp == null)
                    throw new ArgumentException($"Failed to find field {name} on {source.FullName}");
                lock (m_lock)
                    return m_memberStringCache[key] = tmp;
            }
            else
                throw new ArgumentException("Failed to parse definition");
        }

        /// <summary>
        /// Returns a value indicating if the given type is a struct with a sequential layout
        /// </summary>
        /// <returns><c>true</c>, if <paramref name="it"/> is a simple struct, <c>false</c> otherwise.</returns>
        /// <param name="it">The item type to check.</param>
        private static bool IsSimpleStruct(Type it)
        {
            return it.IsValueType && (it.IsLayoutSequential || it.IsExplicitLayout);
        }

        /// <summary>
        /// Checks if the given type is one of the basic serialization types
        /// </summary>
        /// <returns><c>true</c>, if the type is a serializable primitive type, <c>false</c> otherwise.</returns>
        /// <param name="type">The type to check.</param>
        public static bool IsSerializationPrimitive(Type type)
        {
            if (PRIMITIVE_TYPES.Contains(type) || type == BinaryConverterStream.RUNTIMETYPE || typeof(Exception).IsAssignableFrom(type))
                return true;

            if (type.IsArray)
                return IsSerializationPrimitive(type.GetElementType());

            if (type.IsGenericType)
            {
                var gt = type.GetGenericTypeDefinition();
                if (
                    gt == typeof(KeyValuePair<,>) ||
                    gt == typeof(Tuple<>) ||
                    gt == typeof(Tuple<,>) ||
                    gt == typeof(Tuple<,,>) ||
                    gt == typeof(Tuple<,,,>) ||
                    gt == typeof(Tuple<,,,,>) ||
                    gt == typeof(Tuple<,,,,,>) ||
                    gt == typeof(Tuple<,,,,,,>) ||
                    gt == typeof(Tuple<,,,,,,,>) ||
                    gt == typeof(List<>) ||
                    gt == typeof(Dictionary<,>))
                {
                    return true;
                }
            }

            return false;
        }

        /// <summary>
        /// Gets the serialization action for a given type
        /// </summary>
        /// <returns>The serialization action.</returns>
        /// <param name="type">The type to serialize.</param>
        public SerializationAction GetAction(Type type)
        {
            lock (m_lock)
            {
                if (m_customSerializer.ContainsKey(type))
                    return SerializationAction.Decompose;

                if (!m_serializationOverrides.TryGetValue(type, out var action))
                    action = SerializationAction.Default;

                if (IsSerializationPrimitive(type) && action != SerializationAction.Fail)
                    return SerializationAction.Decompose;

                if (action == SerializationAction.Default)
                {
                    if (type.IsArray)
                        return GetAction(type.GetElementType());

                    if (type.IsGenericType)
                    {
                        var gt = type.GetGenericTypeDefinition();
                        if (type.GetGenericArguments().Select(GetAction).Any(x => x == SerializationAction.Fail))
                            return SerializationAction.Fail;

                        if (
                            gt == typeof(KeyValuePair<,>) ||
                            gt == typeof(Tuple<>) ||
                            gt == typeof(Tuple<,>) ||
                            gt == typeof(Tuple<,,>) ||
                            gt == typeof(Tuple<,,,>) ||
                            gt == typeof(Tuple<,,,,>) ||
                            gt == typeof(Tuple<,,,,,>) ||
                            gt == typeof(Tuple<,,,,,,>) ||
                            gt == typeof(Tuple<,,,,,,,>) ||
                            gt == typeof(List<>) ||
                            gt == typeof(Dictionary<,>))
                        {
                            return SerializationAction.Decompose;
                        }
                    }

                    if (type.IsValueType)
                        action = SerializationAction.Decompose;
                    else
                        action = SerializationAction.Fail;
                }

                return action;
            }
        }

        /// <summary>
        /// Breaks an object into its individual fields, and returns the types and values for that entry
        /// </summary>
        /// <returns>The dismantled components.</returns>
        /// <param name="item">The struct to disassemble.</param>
        public Tuple<Type[], object[]> SerializeObject(object item)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));

            var it = item.GetType();

            // If we have custom handling, use that
            Func<Type, object, Tuple<Type[], object[]>> serializer;
            lock (m_lock)
                m_customSerializer.TryGetValue(it, out serializer);
            
            if (serializer != null && !PRIMITIVE_TYPES.Contains(it))
                return serializer(it, item);

            // Get custom handling, if any
            var action = GetAction(it);
            if (action == SerializationAction.Fail)
                throw new ArgumentException($"The type {it} is not supported for serialization");
            if (action == SerializationAction.Reference)
                throw new ArgumentException($"The type {it} should be passed by reference, not serialized");

            // Don't bother with primitives
            if (IsSerializationPrimitive(it))
                return new Tuple<Type[], object[]>(new Type[] { it }, new object[] { item });

            if ((it.IsByRef || it.IsArray) && it.GetConstructor(Type.EmptyTypes) == null && action == SerializationAction.Decompose)
                throw new ArgumentException($"The type {it} does not have a default constructor");

            var isSimple = IsSimpleStruct(it);

            var fields = it
                .GetFields(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic)
                .Where(x => GetAction(x.FieldType) != SerializationAction.Ignore)
                .ToArray();

            Func<System.Reflection.FieldInfo[], System.Reflection.FieldInfo[]> fieldfilter;
            lock (m_lock)
                m_serializationFilters.TryGetValue(it, out fieldfilter);

            if (fieldfilter != null)
                fields = fieldfilter(fields);
            
            var offset = isSimple ? 0 : 1;
            var tres = new Type[fields.Length + offset];
            var vres = new object[fields.Length + offset];

            // For non-simple structs, we use a name-mapping scheme
            if (!isSimple)
            {
                tres[0] = typeof(string[]);
                vres[0] = fields.Select(x => x.Name).ToArray();
            }

            for (var i = 0; i < fields.Length; i++)
            {
                tres[i + offset] = fields[i].FieldType;
                vres[i + offset] = fields[i].GetValue(item);
            }

            return new Tuple<Type[], object[]>(tres, vres);
        }

        /// <summary>
        /// Re-assembles a dismantled struct
        /// </summary>
        /// <returns>The re-assembled struct.</returns>
        /// <param name="item">The type of the struct to rebuild.</param>
        /// <param name="arguments">The values from the <see cref="SerializeObject(object)"/> call.</param>
        public object DeserializeObject(Type item, object[] arguments)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));
            if (arguments == null)
                throw new ArgumentNullException(nameof(item));
            if (arguments.Length == 0)
                return new object[0];

            if (IsSerializationPrimitive(item))
            {
                if (arguments.Length != 1)
                    throw new ArgumentOutOfRangeException(nameof(arguments), arguments.Length, $"For primitive type {item}, an array of length 1 was expected");
                return arguments[0];
            }

            Func<Type, object[], object> deserializer;
            lock (m_lock)
                m_customDeserializer.TryGetValue(item, out deserializer);
            if (deserializer != null)
                return deserializer(item, arguments);

            if (!item.IsValueType && item.GetConstructor(Type.EmptyTypes) == null)
                throw new ArgumentException($"The type {item} does not have a default constructor");

            var isSimple = IsSimpleStruct(item);
            var offset = isSimple ? 0 : 1;

            var fields = item.GetFields(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic);

            Func<System.Reflection.FieldInfo[], System.Reflection.FieldInfo[]> fieldfilter;
            lock (m_lock)
                m_serializationFilters.TryGetValue(item, out fieldfilter);            
            if (fieldfilter != null)
                fields = fieldfilter(fields);

            if ((fields.Length + offset) != arguments.Length)
                throw new ArgumentOutOfRangeException(nameof(arguments), arguments.Length, $"For type {item}, an array of length {fields.Length} was expected");

            var target = Activator.CreateInstance(item);
            if (isSimple)
            {
                for (var i = 0; i < fields.Length; i++)
                    fields[i].SetValue(target, arguments[i]);
            }
            else
            {
                var names = arguments[0] as string[];
                if (names == null)
                    throw new ArgumentException($"Expected the first entry in {nameof(arguments)} to be a {typeof(string[])} for type {item}");

                for (var i = 0; i < names.Length; i++)
                {
                    var field = item.GetField(names[i]);
                    if (field == null)
                        throw new ArgumentException($"Found no field named {names[i]} in {item}");
                    field.SetValue(target, arguments[i + offset]);
                }
            }

            return target;
        }
    }
}
