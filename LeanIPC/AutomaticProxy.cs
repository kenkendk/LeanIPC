using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Reflection.Emit;
using System.Threading.Tasks;

namespace LeanIPC
{
    /// <summary>
    /// Helper class that can create wrapping proxies for remote references
    /// </summary>
    public static class ProxyCreator
    {
        /// <summary>
        /// The lock guarding <see cref="_interfaceCache"/>
        /// </summary>
        private static readonly object _lock = new object();

        /// <summary>
        /// The interface cache, key is the interface, value is the dynamic type.
        /// </summary>
        private static readonly Dictionary<Type, TypeInfo> _interfaceCache = new Dictionary<Type, TypeInfo>();

        /// <summary>
        /// List of interface types that we ignore when bulding properties and methods, as they are handled in other ways
        /// </summary>
        private static readonly Type[] IGNORETYPES = new Type[] { typeof(IRemoteInstance), typeof(IDisposable) };

        /// <summary>
        /// Creates a proxy class for the given interface that makes remote invocations
        /// </summary>
        /// <returns>The remote proxy.</returns>
        /// <param name="peer">The peer to invoke remote methods on.</param>
        /// <param name="type">The remote type to make the proxy for.</param>
        /// <param name="proxytype">The type to create a proxy with</param>
        /// <param name="handle">The remote handle.</param>
        /// <typeparam name="TBase">The type that the proxy is interfacing with</typeparam>
        public static object CreateAutomaticProxy<TBase>(RPCPeer peer, Type type, Type proxytype, long handle)
            where TBase : IProxyHelper
        {
            return CreateRemoteProxy(peer, type, proxytype, typeof(TBase), handle);
        }

        /// <summary>
        /// Creates a proxy class for the given interface that makes remote invocations
        /// </summary>
        /// <returns>The remote proxy.</returns>
        /// <param name="peer">The peer to invoke remote methods on.</param>
        /// <param name="type">The remote type to make the proxy for.</param>
        /// <param name="proxytype">The type to create a proxy with</param>
        /// <param name="handlertype">The type that the proxy is interfacing with, must implement IProxyHelper</param>
        /// <param name="handle">The remote handle.</param>
        public static object CreateRemoteProxy(RPCPeer peer, Type type, Type proxytype, Type handlertype, long handle)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));
            if (proxytype == null)
                throw new ArgumentNullException(nameof(proxytype));
            if (handlertype == null)
                throw new ArgumentNullException(nameof(handlertype));


            lock (_lock)
            {
                if (!_interfaceCache.ContainsKey(proxytype))
                {
                    if (!handlertype.GetInterfaces().Any(x => x == typeof(IProxyHelper)))
                        throw new Exception($"The type {handlertype} does not implement {typeof(IProxyHelper)}");

                    var typename = "DynamicProxy." + type.FullName + "." + Guid.NewGuid().ToString("N").Substring(0, 6);

                    // Build an assembly and a module to contain the type
                    var assemblyName = new AssemblyName($"{nameof(LeanIPC)}.{nameof(ProxyCreator)}.{typename}");
                    var assembly = AssemblyBuilder.DefineDynamicAssembly(assemblyName, AssemblyBuilderAccess.Run);
                    var module = assembly.DefineDynamicModule(assemblyName.Name);

                    // The RemoteObject constructor args
                    var constructorArgs = new Type[] { typeof(RPCPeer), typeof(Type), typeof(long) };

                    var interfaces = new List<Type>();
                    if (proxytype.IsInterface)
                        interfaces.Add(proxytype);

                    if (!proxytype.GetInterfaces().Contains(typeof(IRemoteInstance)) && handlertype.GetInterfaces().Contains(typeof(IRemoteInstance)))
                        interfaces.Add(typeof(IRemoteInstance));
                    if (!proxytype.GetInterfaces().Contains(typeof(IDisposable)))
                        interfaces.Add(typeof(IDisposable));

                    var basetype = proxytype.IsInterface
                        ? typeof(object)
                        : proxytype;

                    // Create the type definition
                    var typeBuilder = module.DefineType(typename, TypeAttributes.Public, basetype, interfaces.ToArray());

                    // Create a field to store the remote object instance
                    var remotefld = typeBuilder.DefineField("__remoteHandle", handlertype, FieldAttributes.Private | FieldAttributes.InitOnly);

                    // Create the constructor that initializes the remote object field
                    var constructor = typeBuilder.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, constructorArgs);
                    var construtorIL = constructor.GetILGenerator();
                    var conm = handlertype.GetConstructor(BindingFlags.Public | BindingFlags.Instance, null, constructorArgs, null);
                    if (conm == null)
                        throw new Exception($"The type {handlertype} does not have a constructor that takes the types ({typeof(RPCPeer)}, {typeof(Type)}, {typeof(long)})");
                    construtorIL.Emit(OpCodes.Ldarg_0);
                    construtorIL.Emit(OpCodes.Ldarg_1);
                    construtorIL.Emit(OpCodes.Ldarg_2);
                    construtorIL.Emit(OpCodes.Ldarg_3);
                    construtorIL.Emit(OpCodes.Newobj, conm);
                    construtorIL.Emit(OpCodes.Stfld, remotefld);
                    construtorIL.Emit(OpCodes.Ret);

                    // Explicitly wire the IRemoteInstance interface to call remotefld directly
                    // Since we map this directly, we can treat property access as methods
                    var explictMethods =
                        (
                            interfaces.Contains(typeof(IRemoteInstance))
                            ? typeof(IRemoteInstance).GetMethods(BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance)
                            : new MethodInfo[0]
                        )
                        .Concat(
                            typeof(IDisposable).GetMethods(BindingFlags.DeclaredOnly | BindingFlags.Public | BindingFlags.Instance)
                        );

                    foreach (var sourceMethod in explictMethods)
                    {
                        var parameterTypes = sourceMethod.GetParameters().Select(x => x.ParameterType).ToArray();

                        // If the proxy type is IDisposable, we need to call the remote dispose before the local
                        var isSpecialDispose = sourceMethod.DeclaringType == typeof(IDisposable) && proxytype.GetInterfaces().Contains(typeof(IDisposable));

                        // Replicate the source method
                        var method = typeBuilder.DefineMethod(
                            (isSpecialDispose ? string.Empty : sourceMethod.DeclaringType.Name + ".") + sourceMethod.Name,
                            MethodAttributes.Public | MethodAttributes.Final | MethodAttributes.Virtual,
                            CallingConventions.Standard,
                            sourceMethod.ReturnType,
                            parameterTypes
                        );


                        // Write the IL to call the method through the interface
                        var methodIL = method.GetILGenerator();

                        // Invoke dispose on the remote instance first
                        if (isSpecialDispose)
                        {
                            methodIL.Emit(OpCodes.Ldarg_0);
                            methodIL.Emit(OpCodes.Ldfld, remotefld);
                            methodIL.Emit(OpCodes.Ldstr, sourceMethod.Name);
                            EmitTypeArray(methodIL, parameterTypes);
                            EmitArgumentArray(methodIL, parameterTypes, 1);

                            var methods = handlertype
                                .GetMethods(BindingFlags.Public | BindingFlags.Instance)
                                .Where(x => x.Name == nameof(IProxyHelper.HandleInvokeMethod));

                            methodIL.Emit(OpCodes.Callvirt, methods.First());
                            methodIL.Emit(OpCodes.Pop);
                        }

                        // Pass the call to the IProxyHelper instance
                        methodIL.Emit(OpCodes.Ldarg_0);
                        methodIL.Emit(OpCodes.Ldfld, remotefld);
                        for (var i = 0; i < parameterTypes.Length; i++)
                            EmitLdarg(methodIL, i + 1);
                        methodIL.Emit(OpCodes.Callvirt, sourceMethod);
                        methodIL.Emit(OpCodes.Ret);

                        // Specify that our specially named method implements the interface method
                        if (!isSpecialDispose)
                            typeBuilder.DefineMethodOverride(method, sourceMethod);
                    }


                    // Add all methods
                    foreach (var sourceMethod in AllImplementedMethods(proxytype, IGNORETYPES))
                    {                        
                        var parameterTypes = sourceMethod.GetParameters().Select(x => x.ParameterType).ToArray();

                        // Replicate the source method
                        var method = typeBuilder.DefineMethod(
                            sourceMethod.Name,
                            MethodAttributes.Public | MethodAttributes.HideBySig | MethodAttributes.Final | MethodAttributes.Virtual,
                            CallingConventions.Standard,
                            sourceMethod.ReturnType,
                            parameterTypes
                        );
                        var methodIL = method.GetILGenerator();
                        methodIL.Emit(OpCodes.Ldarg_0);
                        methodIL.Emit(OpCodes.Ldfld, remotefld);
                        methodIL.Emit(OpCodes.Ldstr, sourceMethod.Name);
                        EmitTypeArray(methodIL, parameterTypes);
                        EmitArgumentArray(methodIL, parameterTypes, 1);

                        var isAsyncResult = 
                            sourceMethod.ReturnType == typeof(Task) 
                            || 
                            (sourceMethod.ReturnType.IsConstructedGenericType && sourceMethod.ReturnType.GetGenericTypeDefinition() == typeof(Task<>));

                        var methods = handlertype
                            .GetMethods(BindingFlags.Public | BindingFlags.Instance)
                            .Where(x => x.Name == (isAsyncResult ? nameof(IProxyHelper.HandleInvokeMethodAsync) : nameof(IProxyHelper.HandleInvokeMethod)));

                        if (sourceMethod.ReturnType == typeof(void))
                        {
                            // We discard the null object from the method
                            methodIL.Emit(OpCodes.Call, methods.Where(x => !x.IsGenericMethodDefinition).First());
                            methodIL.Emit(OpCodes.Pop);
                        }
                        else if (sourceMethod.ReturnType == typeof(Task))
                        {
                            // We return the Task from the underlying call
                            methodIL.Emit(OpCodes.Call, methods.Where(x => !x.IsGenericMethodDefinition).First());
                        }
                        else
                        {
                            // Unwrap the Task<T>, so the generic argument is T
                            // or just use the return type for the method
                            var genericParameter =
                                sourceMethod.ReturnType.IsConstructedGenericType && sourceMethod.ReturnType.GetGenericTypeDefinition() == typeof(Task<>)
                                ? sourceMethod.ReturnType.GetGenericArguments().First()
                                : sourceMethod.ReturnType;

                            methodIL.Emit(OpCodes.Call, methods.Where(x => x.IsGenericMethodDefinition).First().MakeGenericMethod(genericParameter));
                        }
                        methodIL.Emit(OpCodes.Ret);
                    }

                    // Add all properties
                    foreach (var sourceProperty in AllImplementedProperties(proxytype, IGNORETYPES))
                    {
                        var indexParameters = sourceProperty.GetIndexParameters().Select(x => x.ParameterType).ToArray();

                        var property = typeBuilder.DefineProperty(
                            sourceProperty.Name,
                            PropertyAttributes.None,
                            sourceProperty.PropertyType,
                            indexParameters
                        );
                        if (sourceProperty.CanRead)
                        {
                            var getMethod = typeBuilder.DefineMethod(
                                "get_" + property.Name,
                                MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig | MethodAttributes.Final | MethodAttributes.Virtual | MethodAttributes.NewSlot,
                                CallingConventions.HasThis,
                                sourceProperty.PropertyType,
                                indexParameters
                            );

                            var getMethodIL = getMethod.GetILGenerator();
                            getMethodIL.Emit(OpCodes.Ldarg_0);
                            getMethodIL.Emit(OpCodes.Ldfld, remotefld);
                            getMethodIL.Emit(OpCodes.Ldstr, sourceProperty.Name);
                            EmitTypeArray(getMethodIL, indexParameters);
                            EmitArgumentArray(getMethodIL, indexParameters, 1);

                            var isAsyncResult =
                                sourceProperty.PropertyType.IsConstructedGenericType
                                &&
                                sourceProperty.PropertyType.GetGenericTypeDefinition() == typeof(Task<>);

                            var baseGetMethod = handlertype
                                .GetMethods(BindingFlags.Public | BindingFlags.Instance)
                                .Where(x => x.Name == (isAsyncResult ? nameof(IProxyHelper.HandleInvokePropertyGetAsync) : nameof(IProxyHelper.HandleInvokePropertyGet)))
                                .Where(x => x.IsGenericMethodDefinition)
                                .First();

                            var genericParameter =
                                isAsyncResult
                                ? sourceProperty.PropertyType.GetGenericArguments().First()
                                : sourceProperty.PropertyType;

                            getMethodIL.Emit(OpCodes.Call, baseGetMethod.MakeGenericMethod(genericParameter));
                            getMethodIL.Emit(OpCodes.Ret);

                            property.SetGetMethod(getMethod);
                        }

                        if (sourceProperty.CanWrite)
                        {
                            var setMethod = typeBuilder.DefineMethod(
                                "set_" + property.Name,
                                MethodAttributes.Public | MethodAttributes.SpecialName | MethodAttributes.HideBySig | MethodAttributes.Final | MethodAttributes.Virtual | MethodAttributes.NewSlot,
                                CallingConventions.HasThis,
                                typeof(void),
                                new Type[] { sourceProperty.PropertyType }.Concat(indexParameters).ToArray()
                            );

                            var setMethodIL = setMethod.GetILGenerator();
                            setMethodIL.Emit(OpCodes.Ldarg_0);
                            setMethodIL.Emit(OpCodes.Ldfld, remotefld);
                            setMethodIL.Emit(OpCodes.Ldstr, sourceProperty.Name);
                            setMethodIL.Emit(OpCodes.Ldarg_1);
                            if (sourceProperty.PropertyType.IsValueType)
                                setMethodIL.Emit(OpCodes.Box, sourceProperty.PropertyType);

                            EmitTypeArray(setMethodIL, indexParameters);
                            EmitArgumentArray(setMethodIL, indexParameters, 2);

                            setMethodIL.Emit(OpCodes.Call, handlertype.GetMethod(nameof(IProxyHelper.HandleInvokePropertySet), BindingFlags.Public | BindingFlags.Instance));
                            setMethodIL.Emit(OpCodes.Ret);

                            property.SetSetMethod(setMethod);
                        }
                    }

                    _interfaceCache[proxytype] = typeBuilder.CreateTypeInfo();
                }
            }

            // Return the instance
            //var typeInfo = typeBuilder.CreateTypeInfo();
            //var instanceConstructor = typeInfo.GetConstructor(constructorArgs);
            return Activator.CreateInstance(_interfaceCache[proxytype], new object[] { peer, type, handle });
        }

        /// <summary>
        /// Returns all interfaces from the source interface
        /// </summary>
        /// <returns>The interfaces.</returns>
        /// <param name="source">The interface to start with.</param>
        /// <param name="ignoretypes">List of interfaces to ignore</param>
        private static IEnumerable<Type> AllTypes(Type source, params Type[] ignoretypes)
        {
            var stack = new Stack<Type>();
            var visited = new HashSet<Type>();
            stack.Push(source);

            if (ignoretypes != null)
                foreach (var t in ignoretypes)
                    visited.Add(t);

            while (stack.Count != 0)
            {
                var cur = stack.Pop();
                if (!visited.Contains(cur))
                {
                    // This is a new interface
                    yield return cur;

                    // Don't visit this again
                    visited.Add(cur);

                    // Return all the interfaces here
                    foreach (var n in cur.GetInterfaces())
                        // Don't bother storing them, if we've already been there
                        if (!visited.Contains(n))
                            stack.Push(n);
                }
            }
        }

        /// <summary>
        /// Returns all the implemented methods.
        /// </summary>
        /// <returns>The implemented methods.</returns>
        /// <param name="starttype">The interface to look for methods in.</param>
        /// <param name="ignoretypes">A list of interfaces to ignore</param>
        private static IEnumerable<MethodInfo> AllImplementedMethods(Type starttype, params Type[] ignoretypes)
        {
            // Keep track of methods we have already emitted
            var visited = new HashSet<string>();

            foreach (var type in AllTypes(starttype, ignoretypes))
            {
                foreach (var method in type.GetMethods(BindingFlags.Public | BindingFlags.Instance))
                {
                    // Only return implementable methods
                    if (starttype.IsInterface || method.IsVirtual)
                    {
                        var signature = string.Join(":",
                            new string[] {
                            method.ReturnType.FullName,
                            method.Name,
                            }
                            .Concat(
                                method.GetParameters().Select(x => x.ParameterType.FullName)
                            )
                        );

                        if (!visited.Contains(signature))
                        {
                            visited.Add(signature);

                            // Skip the methods if they are just property accessors
                            if (type.GetProperties().Any(x => x.SetMethod == method || x.GetMethod == method))
                                continue;

                            yield return method;
                        }
                    }
                }
            }                
        }

        /// <summary>
        /// Returns all the implemented methods.
        /// </summary>
        /// <returns>The implemented methods.</returns>
        /// <param name="starttype">The interface to look for methods in.</param>
        /// <param name="ignoretypes">A list of interfaces to ignore</param>
        private static IEnumerable<PropertyInfo> AllImplementedProperties(Type starttype, params Type[] ignoretypes)
        {
            // Keep track of methods we have already emitted
            var visited = new HashSet<string>();

            foreach (var type in AllTypes(starttype, ignoretypes))
            {
                foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
                {
                    var signature = string.Join(":",
                        new string[] {
                            property.PropertyType.FullName,
                            property.Name,
                        }
                        .Concat(
                            property.GetIndexParameters().Select(x => x.ParameterType.FullName)
                        )
                    );

                    if (!visited.Contains(signature))
                    {
                        visited.Add(signature);
                        yield return property;
                    }
                }
            }
        }

        /// <summary>
        /// Creates an object array with the arguments to the function
        /// </summary>
        /// <param name="generator">The IL generator to use.</param>
        /// <param name="parameterTypes">The types of the parameters</param>
        /// <param name="offset">The argument offset, should be <c>1</c> if the method is not static.</param>
        /// <param name="useNullForEmpty">If <c>true</c>, emits a null value instead of an empty array</param>
        private static void EmitArgumentArray(ILGenerator generator, Type[] parameterTypes, int offset, bool useNullForEmpty = true)
        {
            if (parameterTypes == null || (parameterTypes.Length == 0 && useNullForEmpty))
            {
                generator.Emit(OpCodes.Ldnull);
            }
            else
            {
                EmitLdcI4(generator, parameterTypes.Length);
                generator.Emit(OpCodes.Newarr, typeof(object));

                for (var i = 0; i < parameterTypes.Length; i++)
                {
                    generator.Emit(OpCodes.Dup);
                    EmitLdcI4(generator, i);
                    EmitLdarg(generator, i + offset);
                    if (parameterTypes[i].IsValueType)
                        generator.Emit(OpCodes.Box, parameterTypes[i]);
                    generator.Emit(OpCodes.Stelem_Ref);
                }
            }
        }

        /// <summary>
        /// Helper method to emit a type array
        /// </summary>
        /// <param name="generator">The IL generator to use.</param>
        /// <param name="types">The array to output.</param>
        /// <param name="useNullForEmpty">If set to <c>true</c>, empty arrays are emitted as <c>null</c> values.</param>
        private static void EmitTypeArray(ILGenerator generator, Type[] types, bool useNullForEmpty = true)
        {
            // Avoid the overhead if possible
            if (types == null || (useNullForEmpty && types.Length == 0))
            {
                generator.Emit(OpCodes.Ldnull);
                return;
            }

            // Create the type array
            EmitLdcI4(generator, types.Length);
            generator.Emit(OpCodes.Newarr, typeof(Type));

            // For each of the items, do
            for (var i = 0; i < types.Length; i++)
            {
                // Copy the array reference for storing
                generator.Emit(OpCodes.Dup);
                EmitLdcI4(generator, i);
                // Get the type
                generator.Emit(OpCodes.Ldtoken, types[i]);
                generator.Emit(OpCodes.Call, typeof(Type).GetMethod(nameof(Type.GetTypeFromHandle)));
                // Store it in the array
                generator.Emit(OpCodes.Stelem_Ref);
            }

            // The array is now the first element on the stack
        }

        /// <summary>
        /// Helper method to emit a constant integer load,
        /// using a short opcode if possible
        /// </summary>
        /// <param name="generator">The IL generator to emit with.</param>
        /// <param name="value">The constant value to emit.</param>
        private static void EmitLdcI4(ILGenerator generator, int value)
        {
            switch (value)
            {
                case 0:
                    generator.Emit(OpCodes.Ldc_I4_0);
                    break;
                case 1:
                    generator.Emit(OpCodes.Ldc_I4_1);
                    break;
                case 2:
                    generator.Emit(OpCodes.Ldc_I4_2);
                    break;
                case 3:
                    generator.Emit(OpCodes.Ldc_I4_3);
                    break;
                case 4:
                    generator.Emit(OpCodes.Ldc_I4_4);
                    break;
                case 5:
                    generator.Emit(OpCodes.Ldc_I4_5);
                    break;
                case 6:
                    generator.Emit(OpCodes.Ldc_I4_6);
                    break;
                case 7:
                    generator.Emit(OpCodes.Ldc_I4_7);
                    break;
                case 8:
                    generator.Emit(OpCodes.Ldc_I4_8);
                    break;
                case -1:
                    generator.Emit(OpCodes.Ldc_I4_M1);
                    break;
                default:
                    generator.Emit(OpCodes.Ldc_I4, value);
                    break;
            }
        }

        /// <summary>
        /// Helper method to emit a argument load,
        /// using a short opcode if possible
        /// </summary>
        /// <param name="generator">The IL generator to emit with.</param>
        /// <param name="argno">The argument number to load.</param>
        private static void EmitLdarg(ILGenerator generator, int argno)
        {
            switch (argno)
            {
                case 0:
                    generator.Emit(OpCodes.Ldarg_0);
                    break;
                case 1:
                    generator.Emit(OpCodes.Ldarg_1);
                    break;
                case 2:
                    generator.Emit(OpCodes.Ldarg_2);
                    break;
                case 3:
                    generator.Emit(OpCodes.Ldarg_3);
                    break;
                default:
                    generator.Emit(OpCodes.Ldarg, argno);
                    break;
            }
        }
    }

    /// <summary>
    /// Class for creating a remote proxy
    /// </summary>
    public static class AutomaticProxy
    {
        /// <summary>
        /// Creates a remote proxy for the given handle
        /// </summary>
        /// <returns>The create.</returns>
        /// <param name="peer">The peer to invoke the method on.</param>
        /// <param name="type">The remote type being wrapped.</param>
        /// <param name="interface">The interface to return</param>
        /// <param name="handle">The remote handle.</param>
        public static IRemoteInstance WrapRemote(this RPCPeer peer, Type type, Type @interface, long handle)
        {
            return (IRemoteInstance)ProxyCreator.CreateAutomaticProxy<RemoteObject>(peer, type, @interface, handle);
        }

        /// <summary>
        /// Creates a proxy instance that wraps a dictionary of properties
        /// </summary>
        /// <returns>The property decomposed instance.</returns>
        /// <param name="peer">The unused peer instance.</param>
        /// <param name="type">The remote type being wrapped.</param>
        /// <param name="interface">The interface presented as the wrapped.</param>
        /// <param name="values">The decomposed property values.</param>
        public static object WrapPropertyDecomposedInstance(this RPCPeer peer, Type type, Type @interface, object[] values)
        {
            var d = new Dictionary<string, object>();
            var names = (string[])values[0];
            for (var i = 0; i < names.Length; i++)
                d[names[i]] = values[i + 1];
            var p = ProxyCreator.CreateAutomaticProxy<PropertyDecomposedObject>(peer, type, @interface, 0);
            var h = p.GetType().GetField("__remoteHandle", BindingFlags.Instance | BindingFlags.NonPublic).GetValue(p) as PropertyDecomposedObject;
            h.m_values = d;
            return p;
        }

    }


}
