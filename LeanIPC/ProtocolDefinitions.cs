using System;
using System.Runtime.InteropServices;

namespace LeanIPC
{
    /// <summary>
    /// The message types
    /// </summary>
    public enum MessageType : byte
    {
        /// <summary>
        /// A request message
        /// </summary>
        Request = 1,
        /// <summary>
        /// A response message
        /// </summary>
        Response = 2,
        /// <summary>
        /// A message that does not generate a response
        /// </summary>
        Passthrough = 3,
        /// <summary>
        /// An error response
        /// </summary>
        Error = 4
    }

    /// <summary>
    /// The commands that can be sent across the inter-process boundary
    /// </summary>
    public enum Command : byte
    {
        /// <summary>
        /// The initial ready request
        /// </summary>
        Ready = 1,

        /// <summary>
        /// A ping request
        /// </summary>
        Ping = 2,

        /// <summary>
        /// A shutdown request
        /// </summary>
        Shutdown = 3,

        /// <summary>
        /// A request to invoke a method remotely
        /// </summary>
        InvokeRemoteMethod = 4,

        /// <summary>
        /// Registers a remote object
        /// </summary>
        RegisterRemoteObject = 5,

        /// <summary>
        /// A request to discard a remote object handle
        /// </summary>
        DetachRemoteObject = 6,

        /// <summary>
        /// A request that transports user data
        /// </summary>
        UserData = 7
    }

    /// <summary>
    /// The message that is sent as the initial handshake
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct ReadyMessage
    {
        /// <summary>
        /// The protocol version.
        /// </summary>
        public readonly byte ProtocolVersion;

        /// <summary>
        /// The library version
        /// </summary>
        public readonly string LibraryVersion;

        /// <summary>
        /// The library name
        /// </summary>
        public readonly string LibraryName;

        /// <summary>
        /// The authentication code.
        /// </summary>
        public readonly string AuthenticationCode;

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.ReadyMessage"/> struct.
        /// </summary>
        /// <param name="authenticationCode">The authentication code to use.</param>
        /// <param name="libraryVersion">The library version.</param>
        /// <param name="libraryName">The library name.</param>
        public ReadyMessage(string authenticationCode, string libraryVersion = null, string libraryName = null)
        {
            var asm = typeof(LeanIPC.ReadyMessage).Assembly;
            ProtocolVersion = ProtocolDefinitions.CURRENT_VERSION;
            LibraryVersion = string.IsNullOrWhiteSpace(libraryVersion) ? asm.GetName().Version.ToString() : libraryVersion;
            LibraryName = string.IsNullOrWhiteSpace(libraryName) ? asm.FullName : libraryName;
            AuthenticationCode = authenticationCode;
        }
    }

    /// <summary>
    /// The ping message used to probe the connection for liveliness
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct PingMessage
    {
        /// <summary>
        /// The time the message was generated
        /// </summary>
        public readonly DateTime When;

        /// <summary>
        /// Initializes a new instance of the <see cref="PingMessage"/> struct.
        /// </summary>
        /// <param name="when">The timestamp to use.</param>
        private PingMessage(DateTime when)
        {
            When = when;
        }

        /// <summary>
        /// Creates a fresh ping message.
        /// </summary>
        /// <returns>The ping message.</returns>
        public static PingMessage CreateMessage()
        {
            return new PingMessage(DateTime.Now);
        }
    }

    /// <summary>
    /// The message that is sent as a shutdown request/reply
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct ShutdownMessage
    {
        // Intentionally empty message
    }

    /// <summary>
    /// Represents a request to invoke a remote method.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct InvokeRemoteMethodRequest
    {
        /// <summary>
        /// The remote item handle
        /// </summary>
        public readonly long Handle;
        /// <summary>
        /// The type this operation is performed on
        /// </summary>
        public readonly string Type;
        /// <summary>
        /// The method to invoke.
        /// </summary>
        public readonly string Method;
        /// <summary>
        /// A flag indicating if the request is a read or write request.
        /// Only used if the method is a property or a field.
        /// </summary>
        public readonly bool IsWrite;
        /// <summary>
        /// The argument types
        /// </summary>
        public readonly Type[] Types;
        /// <summary>
        /// The arguments passed to the remote method
        /// </summary>
        public readonly object[] Arguments;

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.InvokeRemoteMethodRequest"/> struct.
        /// </summary>
        /// <param name="handle">The handle for the remote item</param>
        /// <param name="type">The type to invoke the method on.</param>
        /// <param name="method">The method to invoke.</param>
        /// <param name="isWrite">If set to <c>true</c>, the request is a property or field write request .</param>
        /// <param name="arguments">The method arguments.</param>
        public InvokeRemoteMethodRequest(long handle, string type, string method, bool isWrite, Type[] types, object[] arguments)
        {
            Handle = handle;
            Type = type;
            Method = method;
            IsWrite = isWrite;
            Types = types;
            Arguments = arguments;
        }
    }

    /// <summary>
    /// Represents a response to a remote method invocation.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct InvokeRemoteMethodResponse
    {
        /// <summary>
        /// The result data type
        /// </summary>
        public readonly Type ResultType;

        /// <summary>
        /// The result.
        /// </summary>
        public object Result;

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.InvokeRemoteMethodResponse"/> struct.
        /// </summary>
        /// <param name="resultType">The result data type</param>
        /// <param name="result">The method invocation result.</param>
        public InvokeRemoteMethodResponse(Type resultType, object result)
        {
            ResultType = resultType;
            Result = result;
        }
    }

    /// <summary>
    /// Represents a request to register a remote object for future use
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct RegisterRemoteObjectRequest
    {
        /// <summary>
        /// The type of the object to register
        /// </summary>
        public readonly Type ObjectType;

        /// <summary>
        /// The handle for the item to register
        /// </summary>
        public readonly long ID;

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.RegisterRemoteObjectRequest"/> struct.
        /// </summary>
        /// <param name="type">The type of the object to create.</param>
        /// <param name="id">The object ID.</param>
        public RegisterRemoteObjectRequest(Type type, long id)
        {
            ObjectType = type;
            ID = id;
        }
    }

    /// <summary>
    /// Represents a request to detach an existing remote object
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    public struct DetachRemoteObjectRequest
    {
        /// <summary>
        /// The ID of the item to detach
        /// </summary>
        public readonly long ID;

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.DetachRemoteObjectRequest"/> struct.
        /// </summary>
        /// <param name="id">The ID of the remote object.</param>
        public DetachRemoteObjectRequest(long id)
        {
            ID = id;
        }
    }

    /// <summary>
    /// Basic protocol constants
    /// </summary>
    public static class ProtocolDefinitions
    {
        /// <summary>
        /// The current version (should be the max version).
        /// </summary>
        public const byte CURRENT_VERSION = 1;
        /// <summary>
        /// The maximum allowed version (should be same as current)
        /// </summary>
        public const byte MAX_VERSION = 1;
        /// <summary>
        /// The minimum allowed version supported
        /// </summary>
        public const byte MIN_VERSION = 1;

    }
}
