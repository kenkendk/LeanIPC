using System;
using System.Threading.Tasks;

namespace LeanIPC
{
    /// <summary>
    /// The definition of a request handler
    /// </summary>
    public delegate Task<RequestHandlerResponse> RequestHandler(ParsedMessage message);

    /// <summary>
    /// The respose from a request
    /// </summary>
    public struct RequestHandlerResponse
    {
        /// <summary>
        /// The types to transmit
        /// </summary>
        public readonly Type[] Types;
        /// <summary>
        /// The values to transmit
        /// </summary>
        public readonly object[] Values;

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.RequestHandlerResponse"/> struct.
        /// </summary>
        /// <param name="types">The types to return.</param>
        /// <param name="values">The values to return.</param>
        public RequestHandlerResponse(Type[] types, object[] values)
        {
            Types = types;
            Values = values;
        }

        /// <summary>
        /// Creates a new request handler response for a given type
        /// </summary>
        /// <returns>The <see cref="RequestHandlerResponse"/> instance.</returns>
        /// <param name="value">The value to assign.</param>
        /// <typeparam name="T">The value type parameter.</typeparam>
        public static RequestHandlerResponse FromResult<T>(T value)
        {
            return new RequestHandlerResponse(new Type[] { typeof(T) }, new object[] { value });
        }
    }

    /// <summary>
    /// Helper class to help route typed messages
    /// </summary>
    public class RequestHandlerRouter
    {
    }
}
