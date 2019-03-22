using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;

namespace LeanIPC
{
    /// <summary>
    /// A peer that supports remote method invocations over an instance of <see cref="InterProcessConnection"/>
    /// </summary>
    public class RPCPeer : IDisposable
    {
        /// <summary>
        /// The connection instance
        /// </summary>
        protected readonly InterProcessConnection m_connection;

        /// <summary>
        /// The remote object manager
        /// </summary>
        protected readonly RemoteObjectHandler m_remoteObjects;

        /// <summary>
        /// The type serializer
        /// </summary>
        protected readonly TypeSerializer m_typeSerializer;

        /// <summary>
        /// The allowed remote invocation types
        /// </summary>
        protected readonly Dictionary<Type, Func<System.Reflection.MemberInfo, object[], bool>> m_allowedTypes = new Dictionary<Type, Func<System.Reflection.MemberInfo, object[], bool>>();

        /// <summary>
        /// The lock guarding the <see cref="m_remoteProxies"/> list
        /// </summary>
        private readonly object m_lock = new object();

        /// <summary>
        /// The list of methods used to create a proxy instance from a remote type
        /// </summary>
        private readonly List<Func<RPCPeer, Type, long, IRemoteInstance>> m_remoteProxies = new List<Func<RPCPeer, Type, long, IRemoteInstance>>();

        /// <summary>
        /// The list of methods used to create a proxy instance from a remote type
        /// </summary>
        private readonly List<Func<object, Type, Task>> m_preSendHooks = new List<Func<object, Type, Task>>();

        /// <summary>
        /// The dictionary with automatic proxy generators
        /// </summary>
        private readonly Dictionary<Type, Type> m_automaticProxies = new Dictionary<Type, Type>();

        /// <summary>
        /// A global filter that is invoked before the type specific filters.
        /// If this is <c>null</c>, only type specific filters can allow method or property access.
        /// </summary>
        public Func<System.Reflection.MemberInfo, object[], bool> GlobalFilter { get; set; }

        /// <summary>
        /// Gets the type serializer used for this instance.
        /// </summary>
        public TypeSerializer TypeSerializer => m_typeSerializer;

        /// <summary>
        /// The remote object handler
        /// </summary>
        /// <value>The remote handler.</value>
        public RemoteObjectHandler RemoteHandler => m_remoteObjects;

        /// <summary>
        /// The inter-process connection instance
        /// </summary>
        public InterProcessConnection IPC => m_connection;

        /// <summary>
        /// The main task, assigned when starting the peer
        /// </summary>
        public Task MainTask { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.RPCPeer"/> class.
        /// </summary>
        /// <param name="stream">The communication stream.</param>
        /// <param name="allowedTypes">The types on which remote execution is allowed</param>
        public RPCPeer(Stream stream, Type[] allowedTypes = null, Func<System.Reflection.MemberInfo, object[], bool> filterPredicate = null)
            : this(new InterProcessConnection(stream, stream), allowedTypes, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.RPCPeer"/> class.
        /// </summary>
        /// <param name="reader">The reader stream.</param>
        /// <param name="writer">The writer stream.</param>
        /// <param name="allowedTypes">The types on which remote execution is allowed</param>
        public RPCPeer(Stream reader, Stream writer, Type[] allowedTypes = null, Func<System.Reflection.MemberInfo, object[], bool> filterPredicate = null)
            : this(new InterProcessConnection(reader, writer), allowedTypes, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.RPCPeer"/> class.
        /// </summary>
        /// <param name="connection">The connection to use for invoking methods.</param>
        /// <param name="allowedTypes">The types on which remote execution is allowed</param>
        /// <param name="filterPredicate">A predicate function used to filter which methods can be invoked</param>
        public RPCPeer(InterProcessConnection connection, Type[] allowedTypes, Func<System.Reflection.MemberInfo, object[], bool> filterPredicate)
        {
            m_connection = connection ?? throw new ArgumentNullException(nameof(connection));
            m_connection.AddMessageHandler(HandleMessage);
            if (allowedTypes != null)
            {
                if (allowedTypes.Length != 0 && filterPredicate == null)
                    filterPredicate = (a, b) => true;

                foreach (var at in allowedTypes)
                    m_allowedTypes.Add(at, filterPredicate);
            }

            m_typeSerializer = m_connection.TypeSerializer ?? throw new ArgumentNullException(nameof(m_connection.TypeSerializer));
            m_remoteObjects = m_connection.RemoteHandler ?? throw new ArgumentNullException(nameof(m_connection.RemoteHandler));
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.RPCPeer"/> class.
        /// </summary>
        /// <param name="connection">The connection to use for invoking methods.</param>
        /// <param name="filterPredicate">A predicate function used to filter which methods can be invoked</param>
        public RPCPeer(InterProcessConnection connection, Func<System.Reflection.MemberInfo, object[], bool> filterPredicate)
            : this(connection, null, filterPredicate)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.RPCPeer"/> class.
        /// </summary>
        /// <param name="connection">The connection to use for invoking methods.</param>
        /// <param name="allowedTypes">The types on which remote execution is allowed</param>
        public RPCPeer(InterProcessConnection connection, params Type[] allowedTypes)
            : this(connection, allowedTypes, null)
        {
        }

        /// <summary>
        /// Starts the connection after configuration has completed
        /// </summary>
        /// <param name="asclient">If set to <c>true</c>, connect as a client.</param>
        /// <param name="authenticationHandler">The optional authentication handler.</param>
        /// <returns>The peer instance</returns>
        public virtual RPCPeer Start(bool asclient, IAuthenticationHandler authenticationHandler = null)
        {
            if (MainTask != null)
                throw new InvalidOperationException("Cannot start a RPC peer more than once");

            MainTask = m_connection.RunMainLoopAsync(asclient, authenticationHandler);
            m_connection.WaitForHandshakeAsync().Wait();
            return this;
        }

        /// <summary>
        /// Adds a new proxy generator method
        /// </summary>
        /// <param name="method">The method used to generate remoting proxies.</param>
        /// <returns>The peer instance</returns>
        public RPCPeer AddProxyGenerator(Func<RPCPeer, Type, long, IRemoteInstance> method)
        {
            if (method == null)
                throw new ArgumentNullException(nameof(method));

            lock(m_lock)
                m_remoteProxies.Add(method);

            return this;
        }

        /// <summary>
        /// Adds an automatically generated remoting proxy for the given type
        /// </summary>
        /// <param name="remotetype">The type to generate a proxy for.</param>
        /// <param name="localinterface">The local defined interface for the remote object.</param>
        /// <returns>The peer instance</returns>
        public RPCPeer AddAutomaticProxy(Type remotetype, Type localinterface)
        {
            if (localinterface == null)
                throw new ArgumentNullException(nameof(localinterface));
            if (!localinterface.IsInterface)
                throw new ArgumentException($"The type is not an interface: {localinterface}", nameof(localinterface));

            lock (m_lock)
                m_automaticProxies.Add(remotetype, localinterface);

            return this;
        }

        /// <summary>
        /// Removes the automatic proxy generator for the given type
        /// </summary>
        /// <param name="remotetype">The type to remove the automatic proxy for.</param>
        /// <returns><c>true</c> if the item was removed; <c>false</c> otherwise</returns>
        public bool RemoveAutomaticProxy(Type remotetype)
        {
            lock (m_lock)
                return m_automaticProxies.Remove(remotetype);
        }

        /// <summary>
        /// Adds a type to the list of allowed types
        /// </summary>
        /// <param name="filter">The optional filter used to limit access to the methods and properties in the type</param>
        /// <typeparam name="T">The type to add.</typeparam>
        /// <returns>The peer instance</returns>
        public RPCPeer AddAllowedType<T>(Func<System.Reflection.MemberInfo, object[], bool> filter = null)
        {
            return AddAllowedType(typeof(T), filter);
        }

        /// <summary>
        /// Adds a type to the list of allowed types
        /// </summary>
        /// <param name="localtype">The type to allow calls for.</param>
        /// <param name="filter">The optional filter used to limit access to the methods and properties in the type</param>
        public RPCPeer AddAllowedType(Type localtype, Func<System.Reflection.MemberInfo, object[], bool> filter = null)
        {
            if (filter == null)
                filter = (a, b) => true;

            lock (m_lock)
                m_allowedTypes.Add(localtype, filter);

            return this;
        }

        /// <summary>
        /// Removes an allowed type
        /// </summary>
        /// <returns><c>true</c>, if allowed type was removed, <c>false</c> otherwise.</returns>
        /// <param name="localtype">The local type to use.</param>
        public bool RemoveAllowedType(Type localtype)
        {
            lock (m_lock)
                return m_allowedTypes.Remove(localtype);
        }

        /// <summary>
        /// Removes a proxy generator method
        /// </summary>
        /// <param name="method">The method used to generate remoting proxies.</param>
        /// <returns><c>true</c> if the item was removed from the list, <c>false</c> otherwise</returns>
        public bool RemoveProxyGenerator(Func<RPCPeer, Type, long, IRemoteInstance> method)
        {
            if (method == null)
                throw new ArgumentNullException(nameof(method));

            lock (m_lock)
                return m_remoteProxies.Remove(method);
        }

        /// <summary>
        /// Registers a local object for invocation by the remote.
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="item">The local object to expose to the remote peer.</param>
        /// <param name="type">The type to register the object as, if <c>null</c>, the object type is used</param>
        public async Task RegisterLocalObjectOnRemote(object item, Type type = null)
        {
            if (item == null)
                throw new ArgumentNullException(nameof(item));
            
            if (await m_remoteObjects.RegisterLocalObjectAsync(item))
                await m_connection.SendPassthroughAsync(Command.RegisterRemoteObject, new RegisterRemoteObjectRequest(type ?? item.GetType(), m_remoteObjects.GetLocalHandle(item)));            
        }

        /// <summary>
        /// Invokes a method remotely, and returns the result
        /// </summary>
        /// <returns>The result of the remote invocation.</returns>
        /// <param name="handle">The remote instance handle, or zero for null.</param>
        /// <param name="remotetype">A type descriptor for the type the method is invoked on.</param>
        /// <param name="remotemethod">A serialized description of the method to invoke.</param>
        /// <param name="types">The types of the input arguments.</param>
        /// <param name="arguments">The values given to the method when invoked.</param>
        /// <param name="isWrite">If set to <c>true</c> this is a write field or property request.</param>
        /// <typeparam name="T">The return type</typeparam>
        public async Task<T> InvokeRemoteMethodAsync<T>(long handle, string remotetype, string remotemethod, Type[] types, object[] arguments, bool isWrite)
        {
            return (T)await InvokeRemoteMethodAsync(handle, remotetype, remotemethod, types, arguments, isWrite);
        }

        /// <summary>
        /// Invokes a method remotely, and returns the result
        /// </summary>
        /// <returns>The result of the remote invocation.</returns>
        /// <param name="handle">The remote instance handle, or zero for null.</param>
        /// <param name="remotetype">A type descriptor for the type the method is invoked on.</param>
        /// <param name="remotemethod">A serialized description of the method to invoke.</param>
        /// <param name="types">The types of the input arguments.</param>
        /// <param name="arguments">The values given to the method when invoked.</param>
        /// <param name="isWrite">If set to <c>true</c> this is a write field or property request.</param>
        public async Task<object> InvokeRemoteMethodAsync(long handle, string remotetype, string remotemethod, Type[] types, object[] arguments, bool isWrite)
        {
            if (string.IsNullOrWhiteSpace(remotetype))
                throw new ArgumentNullException(remotetype);
            if (string.IsNullOrWhiteSpace(remotemethod))
                throw new ArgumentNullException(remotemethod);

            types = types ?? new Type[0];
            arguments = arguments ?? new object[0];

            if (types.Length != arguments.Length)
                throw new ArgumentOutOfRangeException(nameof(types), $"The length of  {nameof(types)} must be the same as the length of {nameof(arguments)}");

            // Register each local reference item with the remote
            for (var i = 0; i < arguments.Length; i++)
            {
                var arg = arguments[i];
                if (arg == null)
                    continue;

                var tp = arg.GetType();

                var action = m_typeSerializer.GetAction(tp);
                if (action == SerializationAction.Fail)
                {
                    var ta = m_typeSerializer.GetAction(types[i]);
                    if (ta != SerializationAction.Fail)
                    {
                        action = ta;
                        tp = types[i];
                    }
                }

                if (action == SerializationAction.Fail)
                    throw new Exception($"Cannot pass item with type {arg.GetType()} ({types[i]})");

                if (m_preSendHooks.Count > 0)
                    await Task.WhenAll(m_preSendHooks.Select(x => x(arg, tp)));

                if (action == SerializationAction.Reference || m_remoteObjects.IsLocalObject(arg))
                    await RegisterLocalObjectOnRemote(arg, tp);
            }

            var res = await m_connection.SendAndWaitAsync(
                Command.InvokeRemoteMethod,
                new InvokeRemoteMethodRequest(handle, remotetype, remotemethod, isWrite, types, arguments)
            );

            if (res.Types.Length == 0 || res.Types[0] == typeof(void))
                return null;

            var resp = (InvokeRemoteMethodResponse)res.Values[0];
            if (resp.ResultType == typeof(void) || resp.ResultType == typeof(Task))
                return null;
            if (resp.ResultType.IsConstructedGenericType && resp.ResultType.GetGenericTypeDefinition() == typeof(Task<>))
                return resp.Result;

            return resp.Result;
        }

        /// <summary>
        /// Invokes a method remotely, and returns the result
        /// </summary>
        /// <returns>The result of the remote invocation.</returns>
        /// <param name="handle">The remote instance handle, or zero for null.</param>
        /// <param name="method">The method to invoke remotely.</param>
        /// <param name="arguments">The values given to the method when invoked.</param>
        /// <param name="isWrite">If set to <c>true</c> this is a write field or property request.</param>
        /// <typeparam name="T">The return type</typeparam>
        public async Task<T> InvokeRemoteMethodAsync<T>(long handle, System.Reflection.MemberInfo method, object[] arguments, bool isWrite)
        {
            return (T)await InvokeRemoteMethodAsync(handle, method, arguments, isWrite);
        }

        /// <summary>
        /// Invokes a method remotely, and returns the result
        /// </summary>
        /// <returns>The result of the remote invocation.</returns>
        /// <param name="handle">The remote instance handle, or zero for null.</param>
        /// <param name="method">The method to invoke remotely.</param>
        /// <param name="arguments">The values given to the method when invoked.</param>
        /// <param name="isWrite">If set to <c>true</c> this is a write field or property request.</param>
        public Task<object> InvokeRemoteMethodAsync(long handle, System.Reflection.MemberInfo method, object[] arguments, bool isWrite)
        {
            var methodstring = m_typeSerializer.GetShortDefinition(method);
            var type = m_typeSerializer.GetShortTypeName(method.DeclaringType);

            Type[] methodtypes;
            if (method is System.Reflection.MethodInfo)
            {
                var mi = method as System.Reflection.MethodInfo;
                methodtypes = mi.GetParameters().Select(x => x.ParameterType).ToArray();
            }
            else if (method is System.Reflection.ConstructorInfo)
            {
                var ci = method as System.Reflection.ConstructorInfo;
                methodtypes = ci.GetParameters().Select(x => x.ParameterType).ToArray();
            }
            else if (method is System.Reflection.FieldInfo)
            {
                if (isWrite)
                    methodtypes = new Type[] { ((System.Reflection.FieldInfo)method).FieldType };
                else
                    methodtypes = new Type[0];
            }
            else if (method is System.Reflection.PropertyInfo)
            {
                var pi = method as System.Reflection.PropertyInfo;
                var indexTypes = pi.GetIndexParameters().Select(x => x.ParameterType);
                if (isWrite)
                    indexTypes = new Type[] { pi.PropertyType }.Concat(indexTypes);
                
                methodtypes = indexTypes.ToArray();
            }
            else
                throw new Exception($"Method is not supported for invoke: {method}");


            return InvokeRemoteMethodAsync(handle, type, methodstring, methodtypes, arguments, isWrite);
        }

        /// <summary>
        /// Sends a detach message to the remote side, invalidating the handle
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="handle">The remote handle.</param>
        public async Task InvokeDetachAsync(long handle)
        {
            await m_connection.SendPassthroughAsync(Command.DetachRemoteObject, new DetachRemoteObjectRequest(handle));
            await m_remoteObjects.RemoveRemoteHandleAsync(handle);
        }

        /// <summary>
        /// The RPC peer message handler
        /// </summary>
        /// <returns><c>true</c> if the message is handled, <c>false</c> otherwise.</returns>
        /// <param name="message">The message to handle.</param>
        protected async Task<bool> HandleMessage(ParsedMessage message)
        {
            if (message.Command == Command.InvokeRemoteMethod)
            {
                var arg = message.Arguments?.FirstOrDefault();
                if (arg is InvokeRemoteMethodRequest)
                {
                    m_connection.RegisterDanglingTask(
                        Task.Run(() =>
                            HandleRemoteInvocation(message.ID, (InvokeRemoteMethodRequest)arg)
                        )
                    );
                }
                else
                {
                    m_connection.RegisterDanglingTask(
                        Task.Run(() =>
                            m_connection.SendErrorResponseAsync(message.ID, message.Command, new Exception($"Invalid argument for the {nameof(Command.InvokeRemoteMethod)} request"))
                        )
                    );
                }
                return true;
            }
            else if (message.Command == Command.RegisterRemoteObject)
            {
                var arg = message.Arguments?.FirstOrDefault();
                try
                {
                    if (arg is RegisterRemoteObjectRequest)
                    {
                        var req = (RegisterRemoteObjectRequest)arg;
                        await m_remoteObjects.RegisterRemoteObjectAsync(req.ID, CreateRemoteProxy(req.ObjectType, req.ID));
                    }
                    else
                    {
                        throw new Exception($"Invalid argument for the {nameof(Command.RegisterRemoteObject)} request");
                    }
                }
                catch (Exception ex)
                {
                    await m_connection.SendErrorResponseAsync(message.ID, message.Command, ex);
                }
                return true;
            }
            else if (message.Command == Command.DetachRemoteObject)
            {
                var arg = message.Arguments?.FirstOrDefault();
                if (arg is DetachRemoteObjectRequest)
                {
                    var id = ((DetachRemoteObjectRequest)arg).ID;
                    await m_remoteObjects.RemoveLocalHandleAsync(id);
                }
                else
                {
                    System.Diagnostics.Debug.WriteLine($"Invalid argument for the {nameof(Command.DetachRemoteObject)} request");
                }
                return true;
            }

            return false;
        }

        /// <summary>
        /// Creates a remote proxy by invoking all the proxy generator methods
        /// </summary>
        /// <returns>The remote proxy.</returns>
        /// <param name="objectType">The type of proxy to create.</param>
        /// <param name="id">The remote proxy handle.</param>
        private IRemoteInstance CreateRemoteProxy(Type objectType, long id)
        {
            if (m_automaticProxies.TryGetValue(objectType, out var interfaceType))
                return AutomaticProxy.WrapRemote(this, objectType, interfaceType, id);

            for (var i = 0; i < m_remoteProxies.Count; i++)
            {
                Func<RPCPeer, Type, long, IRemoteInstance> method;
                lock (m_lock)
                {
                    if (i >= m_remoteProxies.Count)
                        break;
                    method = m_remoteProxies[i];
                }

                var res = method(this, objectType, id);
                if (res != null)
                    return res;
            }

            throw new Exception($"Cannot generate a proxy for type: {objectType}");
        }

        /// <summary>
        /// Handles the remote invocation request, can be overridden.
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="requestId">The requestID to respond to.</param>
        /// <param name="req">The invocation request.</param>
        protected virtual async Task HandleRemoteInvocation(long requestId, InvokeRemoteMethodRequest req)
        {
            // Flag to avoid sending communication errors over a failed link
            var hasResponded = false;

            try
            {
                var type = m_typeSerializer.ParseShortTypeName(req.Type);
                if (!IsTypeAllowed(type))
                    throw new Exception("Operations on that type are not allowed");

                var methodDefinition = m_typeSerializer.GetFromShortDefinition(type, req.Method);

                if (!IsMethodAllowed(methodDefinition, req.Arguments))
                    throw new Exception("Method not allowed");

                Type resType;

                if (methodDefinition is System.Reflection.MethodInfo)
                    resType = ((System.Reflection.MethodInfo)methodDefinition).ReturnType;
                else if (methodDefinition is System.Reflection.ConstructorInfo)
                    resType = type;
                else if (methodDefinition is System.Reflection.PropertyInfo)
                {
                    if (req.IsWrite)
                        resType = typeof(void);
                    else
                        resType = ((System.Reflection.PropertyInfo)methodDefinition).PropertyType;
                }
                else if (methodDefinition is System.Reflection.FieldInfo)
                {
                    if (req.IsWrite)
                        resType = typeof(void);
                    else
                        resType = ((System.Reflection.FieldInfo)methodDefinition).FieldType;
                }
                else
                    throw new Exception($"The reflected item is not supported: {methodDefinition}");

                object source = null;
                if (req.Handle != 0 && !m_remoteObjects.TryGetLocalObject(req.Handle, out source))
                    throw new Exception("The requested handle was not valid");

                object res = InvokeMethod(methodDefinition, source, req.Arguments, req.IsWrite);

                // We perform async tasks locally, and return the unwrapped result
                if (res != null && res is Task)
                {
                    await (Task)res;

                    // Unwrap the Task<T> value
                    if (res.GetType().IsConstructedGenericType)
                    {
                        res = res.GetType().GetProperty("Result").GetValue(res);

                        if (resType.IsConstructedGenericType)
                            resType = resType.GetGenericArguments().First();
                        else
                            resType = typeof(void);
                    }
                    else
                    {
                        res = true;
                        resType = typeof(void);
                    }
                }

                var resAction = res == null ? SerializationAction.Default : m_typeSerializer.GetAction(res.GetType());

                // If we return a value that could be passed by reference, we pass it as that,
                // even tough the actual type is different
                if (res != null && resAction == SerializationAction.Fail)
                {
                    var resTypeAction = m_typeSerializer.GetAction(resType);
                    if (resTypeAction == SerializationAction.Reference)
                        resAction = resTypeAction;
                }

                // We need to explore the result to ensure that we find all reference objects that we need to send
                // This also ensures that we get any errors *before* attempting to send, which
                // gives much nicer error messages

                var items = m_typeSerializer.VisitValues(res, resType).ToList();
                var fails = items.FirstOrDefault(x => x.Item3 == SerializationAction.Fail);

                if (fails != null)
                    throw new ArgumentException($"Not configured to transmit an instance of type {fails.Item1.GetType()}{(fails.Item1.GetType() == fails.Item2 ? "" : $" as ({fails.Item2})")}");

                if (m_preSendHooks.Count > 0)
                    await Task.WhenAll(m_preSendHooks.Select(x => x(res, resType)));

                // If we are sending an object back, register a local reference to it
                foreach (var el in items.Where(x => x.Item3 == SerializationAction.Reference))
                    if (await m_remoteObjects.RegisterLocalObjectAsync(el.Item1))
                        await m_connection.SendPassthroughAsync(Command.RegisterRemoteObject, new RegisterRemoteObjectRequest(el.Item2, m_remoteObjects.GetLocalHandle(el.Item1)));

                hasResponded = true;
                await m_connection.SendResponseAsync(requestId, Command.InvokeRemoteMethod, new InvokeRemoteMethodResponse(resType, res));
            }
            catch (Exception ex)
            {
                if (hasResponded)
                    throw;

                // Unwrap this to get a better error message to pass on
                var x = ex;
                while (x != null && x is System.Reflection.TargetInvocationException)
                    x = ((System.Reflection.TargetInvocationException)x).InnerException;

                await m_connection.SendErrorResponseAsync(requestId, Command.InvokeRemoteMethod, x ?? ex);
            }

        }

        /// <summary>
        /// Plugin method that can be overriden to filter which types are safe to call.
        /// </summary>
        /// <returns><c>true</c>, if type was allowed, <c>false</c> otherwise.</returns>
        /// <param name="targetType">The type to evaluate.</param>
        protected virtual bool IsTypeAllowed(Type targetType)
        {
            if (GlobalFilter != null)
                return true;

            // If there is a filter but no types, allow the filter to decide
            return m_allowedTypes.ContainsKey(targetType);
        }

        /// <summary>
        /// Plugin method that can be overriden to filter individual methods
        /// </summary>
        /// <returns><c>true</c>, if method was allowed, <c>false</c> otherwise.</returns>
        /// <param name="method">The method to evaluate.</param>
        /// <param name="arguments">The arguments to the method.</param>
        protected virtual bool IsMethodAllowed(System.Reflection.MemberInfo method, object[] arguments)
        {
            if (GlobalFilter != null && !GlobalFilter(method, arguments))
                return false;

            m_allowedTypes.TryGetValue(method.DeclaringType, out var f);
            return f != null && f(method, arguments);
        }

        /// <summary>
        /// Method that does the actual remote invocation. This can be overriden to make a more granular permission system than just filtering by the types.
        /// </summary>
        /// <returns>The result of invoking the method.</returns>
        /// <param name="method">Method.</param>
        /// <param name="instance">Instance.</param>
        /// <param name="isWrite">If set to <c>true</c> is write.</param>
        protected virtual object InvokeMethod(System.Reflection.MemberInfo method, object instance, object[] arguments, bool isWrite)
        {
            if (method is System.Reflection.MethodInfo)
                return ((System.Reflection.MethodInfo)method).Invoke(instance, arguments);
            else if (method is System.Reflection.ConstructorInfo)
                return ((System.Reflection.ConstructorInfo)method).Invoke(arguments);
            else if (method is System.Reflection.PropertyInfo)
            {
                if (((System.Reflection.PropertyInfo)method).GetIndexParameters().Length == 0)
                {
                    if (isWrite)
                    {
                        ((System.Reflection.PropertyInfo)method).SetValue(instance, arguments[0]);
                        return null;
                    }
                    else
                        return ((System.Reflection.PropertyInfo)method).GetValue(instance);
                }
                else
                {
                    if (isWrite)
                    {
                        ((System.Reflection.PropertyInfo)method).SetValue(instance, arguments[0], arguments.Skip(1).ToArray());
                        return null;
                    }
                    else
                    {
                        return ((System.Reflection.PropertyInfo)method).GetValue(instance, arguments);
                    }
                }
            }
            else if (method is System.Reflection.FieldInfo)
            {
                if (isWrite)
                {
                    ((System.Reflection.FieldInfo)method).SetValue(instance, arguments[0]);
                    return null;
                }
                else
                    return ((System.Reflection.FieldInfo)method).GetValue(instance);

            }

            throw new Exception($"The reflected item is not supported: {method}");

        }

        /// <summary>
        /// Disposes the instance
        /// </summary>
        /// <param name="isDisposing">If set to <c>true</c>, the call is from <see cref="Dispose()"/>.</param>
        protected virtual void Dispose(bool isDisposing)
        {
            m_connection?.Dispose();
        }

        /// <summary>
        /// Releases all resource used by the <see cref="T:LeanIPC.RPCClient"/> object.
        /// </summary>
        /// <remarks>Call <see cref="Dispose"/> when you are finished using the <see cref="T:LeanIPC.RPCClient"/>. The
        /// <see cref="Dispose"/> method leaves the <see cref="T:LeanIPC.RPCClient"/> in an unusable state. After
        /// calling <see cref="Dispose"/>, you must release all references to the <see cref="T:LeanIPC.RPCClient"/> so
        /// the garbage collector can reclaim the memory that the <see cref="T:LeanIPC.RPCClient"/> was occupying.</remarks>
        public void Dispose()
        {
            Dispose(true);
        }

        /// <summary>
        /// Adds a pre-send hook method
        /// </summary>
        /// <returns>Thepeer.</returns>
        /// <param name="callback">The callback function that gets the object and the resolved type.</param>
        public RPCPeer AddPreSendHook(Func<object, Type, Task> callback)
        {
            lock (m_lock)
                m_preSendHooks.Add(callback ?? throw new ArgumentNullException(nameof(callback)));

            return this;
        }

        /// <summary>
        /// Removes a pre send hook.
        /// </summary>
        /// <returns><c>true</c>, if pre send hook was removed, <c>false</c> otherwise.</returns>
        /// <param name="callback">The hook method to remove.</param>
        public bool RemovePreSendHook(Func<object, Type, Task> callback)
        {
            lock (m_lock)
                return m_preSendHooks.Remove(callback ?? throw new ArgumentNullException(nameof(callback)));
        }

        /// <summary>
        /// Registers a method for deserializing an item
        /// </summary>
        /// <returns>The custom deserializer.</returns>
        /// <param name="method">The method that deserializes.</param>
        /// <typeparam name="T">The type to return parameter.</typeparam>
        public RPCPeer RegisterCustomDeserializer<T>(Type source, Func<object[], T> method)
        {
            TypeSerializer.RegisterCustomSerializer(
                source,
                (a, b) => throw new InvalidOperationException("Serialization not supported"),
                (a, b) => method(b)
            );

            return this;
        }


        /// <summary>
        /// Registers a method for deserializing an item
        /// </summary>
        /// <returns>The custom deserializer.</returns>
        /// <param name="method">The method that deserializes.</param>
        /// <typeparam name="T">The type to return parameter.</typeparam>
        public RPCPeer RegisterCustomDeserializer<T>(Func<object[], T> method)
        {
            TypeSerializer.RegisterCustomSerializer(
                typeof(T),
                (a, b) => throw new InvalidOperationException("Serialization not supported"),
                (a, b) => method(b)
            );

            return this;
        }

        /// <summary>
        /// Registers a method for deserializing an item
        /// </summary>
        /// <returns>The custom deserializer.</returns>
        /// <param name="serializer">The method that serializes.</param>
        /// <param name="deserializer">The optional method that deserializes</param>
        /// <typeparam name="T">The type to register.</typeparam>
        public RPCPeer RegisterCustomSerializer<T>(Func<object, Tuple<Type[], object[]>> serializer, Func<object[], T> deserializer = null)
        {
            TypeSerializer.RegisterCustomSerializer(
                typeof(T),
                (a, b) => serializer(b),
                (a, b) => deserializer == null ? throw new InvalidOperationException("Deserialization not supported") : deserializer(b)
            );

            return this;
        }

        /// <summary>
        /// Stops the server or client
        /// </summary>
        /// <returns>An awaitable task indicating that the server has stopped.</returns>
        public virtual Task StopAsync()
        {
            return Task.WhenAll(
                m_connection.ShutdownAsync(),
                MainTask
            );
        }

        /// <summary>
        /// Invokes a remote static method
        /// </summary>
        /// <returns>The return value from the remote method.</returns>
        /// <param name="method">The method to call.</param>
        /// <param name="args">The arguments passed to the method.</param>
        /// <typeparam name="T">The return type parameter.</typeparam>
        public Task<T> CallRemoteStaticMethod<T>(MethodInfo method, params object[] args)
        {
            return InvokeRemoteMethodAsync<T>(0, method, args, false);
        }

        /// <summary>
        /// Invokes a remote static method
        /// </summary>
        /// <returns>The return value from the remote method.</returns>
        /// <param name="method">The method to call.</param>
        /// <param name="args">The arguments passed to the method.</param>
        /// <typeparam name="T">The return type parameter.</typeparam>
        public Task CallRemoteStaticMethod(MethodInfo method, params object[] args)
        {
            return InvokeRemoteMethodAsync(0, method, args, false);
        }

        /// <summary>
        /// Creates a new remote instance, requires that a proxy generator is present on the peer
        /// </summary>
        /// <returns>The create.</returns>
        /// <param name="type">The type of the remote object to create.</param>
        /// <param name="arguments">The arguments to the constructor.</param>
        /// <typeparam name="T">The return type</typeparam>
        public async Task<T> CreateAsync<T>(Type type, params object[] arguments)
        {
            if (!typeof(T).IsInterface)
                throw new Exception($"Return type {typeof(T)} is not an interface");

            var res = await CreateRemoteInstanceAsync(type, arguments);
            return (T)res;
        }

        /// <summary>
        /// Creates a new remote instance
        /// </summary>
        /// <returns>The create.</returns>
        /// <param name="type">The type of the remote object to create.</param>
        /// <param name="arguments">The arguments to the constructor.</param>
        public Task<IRemoteInstance> CreateRemoteInstanceAsync(Type type, params object[] arguments)
        {
            if (type == null)
                throw new ArgumentNullException(nameof(type));

            arguments = arguments ?? new object[0];
            var argtypes = arguments.Select(x => x?.GetType()).ToArray();

            // Get a list of suitable constructors, given the parameters we have
            var constructors = type
                .GetConstructors()
                .Where(x => x.GetParameters().Length == arguments.Length)
                .Where(c =>
                    c.GetParameters()
                        .Zip(
                            argtypes,
                            (p, a) => new { p = p.ParameterType, a }
                        )
                        .All(x =>
                            (x.a == null && x.p.IsByRef)
                            ||
                            (x.a != null && x.p.IsAssignableFrom(x.a))
                        )
                ).ToArray();

            if (constructors.Length == 0)
                throw new ArgumentException($"No constructor on type {type} matches the given types: ({string.Join(",", argtypes.Select(x => x.Name))})");
            if (constructors.Length != 1)
                throw new ArgumentException($"Found multiple constructors on {type} match the given types: {Environment.NewLine}({string.Join(Environment.NewLine, constructors.Select(n => string.Join(",", n.GetParameters().Select(x => x.ParameterType.Name))))}");

            return CreateRemoteInstanceAsync(constructors.First(), arguments);
        }

        /// <summary>
        /// Creates a new remote proxy
        /// </summary>
        /// <returns>The remote instance.</returns>
        /// <param name="constructor">The constructor to use.</param>
        /// <param name="arguments">The arguments to the constructor.</param>
        public async Task<IRemoteInstance> CreateRemoteInstanceAsync(ConstructorInfo constructor, params object[] arguments)
        {
            return (IRemoteInstance)await InvokeRemoteMethodAsync(0, constructor, arguments, false);
        }

        /// <summary>
        /// Creates a remote instance
        /// </summary>
        /// <returns>The remote instance to create.</returns>
        /// <param name="targettype">The remote type to create</param>
        /// <param name="args">The constructor arguments.</param>
        /// <typeparam name="T">The interface for the remote type.</typeparam>
        public Task<T> CreateRemoteInstanceAsync<T>(Type targettype, params object[] args)
        {
            return this.CreateAsync<T>(targettype, args);
        }

        /// <summary>
        /// Registers a type as being decomposed, i.e. all fields are sent
        /// </summary>
        /// <returns>The RPC peer reference type.</returns>
        /// <typeparam name="T">The type to register.</typeparam>
        public RPCPeer RegisterCompositeType<T>()
        {
            return RegisterCompositeType(typeof(T));
        }

        /// <summary>
        /// Registers a type as being decomposed, i.e. all fields are sent
        /// </summary>
        /// <returns>The RPC peer reference type.</returns>
        /// <param name="t">The type to register</param>
        public RPCPeer RegisterCompositeType(Type t)
        {
            return RegisterByValType(t);
        }

        /// <summary>
        /// Registers a type as being locally served, i.e. can be remotely called
        /// </summary>
        /// <returns>The RPC peer reference type.</returns>
        /// <param name="filter">An optional filter; if <c>null</c> all methods and properties on the object are remote callable.</param>
        /// <typeparam name="T">The type to serve locally.</typeparam>
        public RPCPeer RegisterLocallyServedType<T>(Func<MemberInfo, object[], bool> filter = null)
        {
            return RegisterLocallyServedType(typeof(T), filter);
        }

        /// <summary>
        /// Registers a type as being locally served, i.e. can be remotely called
        /// </summary>
        /// <returns>The RPC peer reference type.</returns>
        /// <param name="t">The type to register</param>
        /// <param name="filter">An optional filter; if <c>null</c> all methods and properties on the object are remote callable.</param>
        /// <typeparam name="T">The type to serve locally.</typeparam>
        public RPCPeer RegisterLocallyServedType(Type t, Func<MemberInfo, object[], bool> filter = null)
        {
            AddAllowedType(t, filter);
            return RegisterByRefType(t);
        }


        /// <summary>
        /// Registers an automatically generated proxy for the given type
        /// </summary>
        /// <returns>The RPC peer reference type.</returns>
        /// <typeparam name="TRemoteType">The remote type ro accept.</typeparam>
        /// <typeparam name="TLocalType">The local proxy type to use.</typeparam>
        public RPCPeer RegisterLocalProxyForRemote<TRemoteType, TLocalType>()
        {
            AddAutomaticProxy(typeof(TRemoteType), typeof(TLocalType));
            return this;
        }

        /// <summary>
        /// Register a type as being passed by reference
        /// </summary>
        /// <returns>The RPC peer reference type.</returns>
        /// <typeparam name="T">The type to register as a reference type.</typeparam>
        public RPCPeer RegisterByRefType<T>()
        {
            return RegisterByRefType(typeof(T));
        }

        /// <summary>
        /// Register a type as being passed by reference
        /// </summary>
        /// <returns>The RPC peer reference type.</returns>
        /// <typeparam name="T">The type to register as a reference type.</typeparam>
        public RPCPeer RegisterByRefType(Type t)
        {
            TypeSerializer.RegisterSerializationAction(t, SerializationAction.Reference);
            return this;
        }

        /// <summary>
        /// Register a type as being passed by value
        /// </summary>
        /// <returns>The RPC peer reference type.</returns>
        /// <typeparam name="T">The type to register as a value type.</typeparam>
        public RPCPeer RegisterByValType(Type t)
        {
            TypeSerializer.RegisterSerializationAction(t, SerializationAction.Decompose);
            return this;
        }

        /// <summary>
        /// Register a type as being passed by value
        /// </summary>
        /// <returns>The RPC peer reference type.</returns>
        /// <typeparam name="T">The type to register as a value type.</typeparam>
        public RPCPeer RegisterByValType<T>()
        {
            return RegisterByValType(typeof(T));
        }

        /// <summary>
        /// Register a type as being ignroe
        /// </summary>
        /// <returns>The RPC peer reference type.</returns>
        /// <typeparam name="T">The type to register as ignored.</typeparam>
        public RPCPeer RegisterIgnoreType<T>()
        {
            return RegisterIgnoreType(typeof(T));
        }

        /// <summary>
        /// Register a type as being ignroe
        /// </summary>
        /// <returns>The RPC peer reference type.</returns>
        /// <typeparam name="T">The type to register as ignored.</typeparam>
        public RPCPeer RegisterIgnoreType(Type t)
        {
            TypeSerializer.RegisterSerializationAction(t, SerializationAction.Ignore);
            return this;
        }

        /// <summary>
        /// Registers an item to be decomposed as its properties
        /// </summary>
        /// <returns>The RPC peer reference type.</returns>
        /// <param name="filter">An optional filter to select properties with.</param>
        /// <typeparam name="T">The type to register a decomposer for.</typeparam>
        public RPCPeer RegisterPropertyDecomposer<T>(Func<PropertyInfo, bool> filter = null)
        {
            TypeSerializer.RegisterPropertyDecomposer<T>(filter);
            return this;
        }
    }
}
