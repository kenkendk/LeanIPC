using System;
namespace LeanIPC
{
    /// <summary>
    /// A parsed message result
    /// </summary>
    public struct ParsedMessage
    {
        /// <summary>
        /// The request or response ID
        /// </summary>
        public readonly long ID;
        /// <summary>
        /// The command issued
        /// </summary>
        public readonly Command Command;
        /// <summary>
        /// The command issued
        /// </summary>
        public readonly MessageType Type;
        /// <summary>
        /// The request or response arguments
        /// </summary>
        public readonly object[] Arguments;
        /// <summary>
        /// The request or response arguments
        /// </summary>
        public readonly Type[] Types;
        /// <summary>
        /// An optional exception, signaling that the operation failed
        /// </summary>
        public readonly Exception Exception;

        /// <summary>
        /// Initializes a new instance of the <see cref="ParsedMessage"/> struct.
        /// </summary>
        /// <param name="type">The message type</param>
        /// <param name="id">The request or response ID.</param>
        /// <param name="command">The command issued.</param>
        /// <param name="arguments">The request or response arguments.</param>
        /// <param name="exception">An optional exception.</param>
        public ParsedMessage(MessageType type, long id, Command command, object[] arguments, Type[] types, Exception exception)
        {
            Type = type;
            ID = id;
            Command = command;
            Arguments = arguments;
            Types = types;
            Exception = exception;
        }
    }}
