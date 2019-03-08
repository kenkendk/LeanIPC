using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace LeanIPC
{
    /// <summary>
    /// A class for signalling that the connection is now closed
    /// </summary>
    public class ConnectionClosedException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.ConnectionClosedException"/> class.
        /// </summary>
        public ConnectionClosedException()
            : base()
        {
        }
    }

    /// <summary>
    /// Helper class for automating and validating messages send accross the inter-process boundary
    /// </summary>
    public static class InterprocessMessages
    {
        /// <summary>
        /// Flag to enable trace
        /// </summary>
        private const bool TRACE = false;

        /// <summary>
        /// Sends a passthrough message over the stream
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="writer">The writer instance.</param>
        /// <param name="requestId">The request ID.</param>
        /// <param name="command">The command to send.</param>
        /// <param name="arguments">The arguments to the command.</param>
        public static Task WritePassthroughAsync(BinaryConverterStream writer, long requestId, Command command, Type[] types, object[] arguments)
        {
            return WriteReqOrRespAsync(writer, MessageType.Passthrough, requestId, command, types, arguments);
        }

        /// <summary>
        /// Sends a passthrough message over the stream
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="writer">The writer instance.</param>
        /// <param name="requestId">The request ID.</param>
        /// <param name="command">The command to send.</param>
        /// <param name="argument">The value to send.</param>
        /// <typeparam name="T">The data type parameter.</typeparam>
        public static Task WritePassthroughAsync<T>(BinaryConverterStream writer, long requestId, Command command, T argument)
        {
            return WriteReqOrRespAsync(writer, MessageType.Passthrough, requestId, command, new Type[] { typeof(T) }, new object[] { argument });
        }

        /// <summary>
        /// Sends a request over the stream
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="writer">The writer instance.</param>
        /// <param name="requestId">The request ID.</param>
        /// <param name="command">The command to send.</param>
        /// <param name="arguments">The arguments to the command.</param>
        public static Task WriteRequestAsync(BinaryConverterStream writer, long requestId, Command command, Type[] types, object[] arguments)
        {
            return WriteReqOrRespAsync(writer, MessageType.Request, requestId, command, types, arguments);
        }

        /// <summary>
        /// Sends a request over the stream
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="writer">The writer instance.</param>
        /// <param name="requestId">The request ID.</param>
        /// <param name="command">The command to send.</param>
        /// <param name="argument">The value to send.</param>
        /// <typeparam name="T">The data type parameter.</typeparam>
        public static Task WriteRequestAsync<T>(BinaryConverterStream writer, long requestId, Command command, T argument)
        {
            return WriteReqOrRespAsync(writer, MessageType.Request, requestId, command, new Type[] { typeof(T) }, new object[] { argument });
        }

        /// <summary>
        /// Sends a response to a previously received request over the stream
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="writer">The writer instance.</param>
        /// <param name="requestId">The request ID.</param>
        /// <param name="command">The command to respond to.</param>
        /// <param name="arguments">The arguments in the response.</param>
        public static Task WriteResponseAsync(BinaryConverterStream writer, long requestId, Command command, Type[] types, object[] arguments)
        {
            return WriteReqOrRespAsync(writer, MessageType.Response, requestId, command, types, arguments);
        }

        /// <summary>
        /// Sends a response to a previously received request over the stream
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="writer">The writer instance.</param>
        /// <param name="requestId">The request ID.</param>
        /// <param name="command">The command to respond to.</param>
        /// <param name="argument">The arguments in the response.</param>
        /// <typeparam name="T">The data type parameter.</typeparam>
        public static Task WriteResponseAsync<T>(BinaryConverterStream writer, long requestId, Command command, T argument)
        {
            return WriteReqOrRespAsync(writer, MessageType.Response, requestId, command, new Type[] { typeof(T) }, new object[] { argument });
        }

        /// <summary>
        /// Sends a response to a previously received request over the stream
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="writer">The writer instance.</param>
        /// <param name="requestId">The request ID.</param>
        /// <param name="command">The command to respond to.</param>
        /// <param name="exception">The exception to respond with.</param>
        public static async Task WriteErrorResponseAsync(BinaryConverterStream writer, long requestId, Command command, Exception exception)
        {
            System.Diagnostics.Trace.WriteLineIf(TRACE, $"{requestId} - Sending error response to: {command}");

            await writer.WriteUInt8Async((byte)MessageType.Error);
            await writer.WriteUInt8Async((byte)command);

            await writer.WriteInt64Async(requestId);
            await writer.WriteExceptionAsync(exception);
            await writer.FlushAsync();
        }

        /// <summary>
        /// Gets the request type for built-in messages
        /// </summary>
        /// <returns>The request message type.</returns>
        /// <param name="command">The command to get the request type for.</param>
        private static Type GetRequestMessageType(Command command)
        {
            switch (command)
            {
                case Command.Ready: return typeof(ReadyMessage);
                case Command.Ping: return typeof(PingMessage);
                case Command.Shutdown: return typeof(ShutdownMessage);
                case Command.InvokeRemoteMethod: return typeof(InvokeRemoteMethodRequest);
                case Command.RegisterRemoteObject: return typeof(RegisterRemoteObjectRequest);
                case Command.DetachRemoteObject: return typeof(DetachRemoteObjectRequest);
                default:
                    throw new ArgumentException($"The command {command} is not supported");
            }
        }

        /// <summary>
        /// Gets the response type for built-in messages
        /// </summary>
        /// <returns>The response message type.</returns>
        /// <param name="command">The command to get the response type for.</param>
        private static Type GetResponseMessageType(Command command)
        {
            switch (command)
            {
                case Command.Ready: return typeof(ReadyMessage);
                case Command.Ping: return typeof(PingMessage);
                case Command.Shutdown: return typeof(ShutdownMessage);
                case Command.InvokeRemoteMethod: return typeof(InvokeRemoteMethodResponse);
                default:
                    throw new ArgumentException($"The command {command} is not supported");
            }
        }

        /// <summary>
        /// Sends a request or response over the stream
        /// </summary>
        /// <returns>An awaitable task.</returns>
        /// <param name="writer">The writer instance.</param>
        /// <param name="messageType">The message type</param>
        /// <param name="requestId">The request ID.</param>
        /// <param name="command">The command to send.</param>
        /// <param name="types">The types of the arguments to send.</param>
        /// <param name="arguments">The arguments to send.</param>
        private static async Task WriteReqOrRespAsync(BinaryConverterStream writer, MessageType messageType, long requestId, Command command, Type[] types, object[] arguments)
        {
            if (writer == null)
                throw new ArgumentNullException(nameof(writer));
            
            types = types ?? new Type[0];
            arguments = arguments ?? new object[0];
            if (types.Length != arguments.Length)
                throw new ArgumentException($"The {nameof(types)} array has length {types.Length} and does not match the {nameof(arguments)} array, which has {arguments.Length}", nameof(types));

            if (messageType == MessageType.Error)
            {
                if (types.Length != 1 || !typeof(Exception).IsAssignableFrom(types[0]))
                    throw new ArgumentException("When writing an error response, the only argument must be the exception");

                await WriteErrorResponseAsync(writer, requestId, command, (Exception)arguments[0]);
                return;
            }

            System.Diagnostics.Trace.WriteLineIf(TRACE, $"{requestId} - Writing header for: {messageType}-{command}");

            await writer.WriteUInt8Async((byte)messageType);
            await writer.WriteUInt8Async(((byte)command));

            System.Diagnostics.Trace.WriteLineIf(TRACE, $"{requestId} - Writing ID header for: {messageType}-{command}");

            await writer.WriteInt64Async(requestId);
            switch (command)
            {
                case Command.Ready:
                case Command.Ping:
                case Command.Shutdown:
                case Command.DetachRemoteObject:
                case Command.RegisterRemoteObject:
                case Command.InvokeRemoteMethod:
                    var targettype = messageType != MessageType.Response ? GetRequestMessageType(command) : GetResponseMessageType(command);

                    if (types.Length != 1 || types[0] != targettype)
                        throw new ArgumentException($"The {command} command can only be sent with a single {targettype} argument");

                    //We rewire the arguments to fit the well-known layout, such that we transmit only the fields, not the struct itself
                    var tmp = writer.TypeSerializer.SerializeObject(arguments[0], targettype);
                    types = tmp.Item2;
                    arguments = tmp.Item3;

                    if (targettype == typeof(InvokeRemoteMethodResponse))
                        types[1] = (Type)arguments[0];

                    break;

                case Command.UserData:
                    break;

                default:
                    throw new ArgumentException($"The value {command} is not a supported command type");
            }

            System.Diagnostics.Trace.WriteLineIf(TRACE, $"{requestId} - Writing record for: {messageType}-{command}");
            await writer.WriteRecordAsync(arguments, types, true);
            System.Diagnostics.Trace.WriteLineIf(TRACE, $"{requestId} - Wrote record for: {messageType}-{command}");

            await writer.FlushAsync();
        }

        /// <summary>
        /// Reads a request or a repsonse from the stream
        /// </summary>
        /// <returns>The parsed message.</returns>
        /// <param name="reader">The reader instances.</param>
        public static async Task<ParsedMessage> ReadRequestOrResponseAsync(BinaryConverterStream reader)
        {
            if (reader == null)
                throw new ArgumentNullException(nameof(reader));

            System.Diagnostics.Trace.WriteLineIf(TRACE, $"Reading header");

            MessageType type;
            try
            {
                type = (MessageType)await reader.ReadUInt8Async();
            }
            catch(EndOfStreamException)
            {
                // If we get EoS during the header, we are cleanly terminated
                throw new ConnectionClosedException();
            }

            var command = (Command)await reader.ReadUInt8Async();
            var id = await reader.ReadInt64Async();

            System.Diagnostics.Trace.WriteLineIf(TRACE, $"{id} - Read header for: {type}-{command}");

            Type[] types;
            if (type == MessageType.Error)
            {
                types = new Type[] { typeof(Exception) };
            }
            else
            {
                System.Diagnostics.Trace.WriteLineIf(TRACE, $"{id} - Reading type header for {type}-{command}");
                var typestr = await reader.ReadStringAsync();

                System.Diagnostics.Trace.WriteLineIf(TRACE, $"{id} - Parsing type header for {type}-{command}: {typestr}");
                types = reader.TypeSerializer.ParseShortTypeDefinition(typestr);
            }


            System.Diagnostics.Trace.WriteLineIf(TRACE, $"{id} - Reading {types} arguments for: {type}-{command}");
            var arguments = await reader.ReadRecordAsync(types);

            if (type != MessageType.Error)
            {
                switch (command)
                {
                    case Command.Ready:
                    case Command.Ping:
                    case Command.Shutdown:
                    case Command.DetachRemoteObject:
                    case Command.RegisterRemoteObject:
                    case Command.InvokeRemoteMethod:
                        // Re-construct the message structure from the fields
                        types = new Type[] { (type == MessageType.Request || type == MessageType.Passthrough) ? GetRequestMessageType(command) : GetResponseMessageType(command) };
                        arguments = new object[] { reader.TypeSerializer.DeserializeObject(types[0], arguments) };
                        break;
                    case Command.UserData:
                        break;
                    default:
                        throw new ArgumentException($"The value {command} is not a supported command type");
                }
            }

            System.Diagnostics.Trace.WriteLineIf(TRACE, $"{id} - Returning message for: {type}-{command}");

            return new ParsedMessage(type, id, command, arguments, types, type == MessageType.Error ? (Exception)arguments[0] : null);
        }
    }
}
