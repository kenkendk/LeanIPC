using System;
namespace LeanIPC
{
    /// <summary>
    /// Authentication handler interface
    /// </summary>
    public interface IAuthenticationHandler
    {
        /// <summary>
        /// Creates the request code.
        /// </summary>
        /// <returns>The request code.</returns>
        string CreateRequest();
        /// <summary>
        /// Creates a response from the input request
        /// </summary>
        /// <returns>The response message.</returns>
        /// <param name="request">The input request.</param>
        string CreateResponse(string request);
        /// <summary>
        /// Validates the request message
        /// </summary>
        /// <returns><c>true</c>, if request was validated, <c>false</c> otherwise.</returns>
        /// <param name="request">The request to validate.</param>
        bool ValidateRequest(string request);
        /// <summary>
        /// Validates the response message
        /// </summary>
        /// <returns><c>true</c>, if response was validated, <c>false</c> otherwise.</returns>
        /// <param name="request">The initial request message</param>
        /// <param name="response">The response to validate.</param>
        bool ValidateResponse(string request, string response);
    }

    /// <summary>
    /// Authentication handler that relies on out-of-band communication of a shared secret.
    /// Note that this handler only authenticates the client by the server, the server is NOT validated.
    /// </summary>
    public class OutOfBandClientOnlyAuthenticationHandler : IAuthenticationHandler
    {
        /// <summary>
        /// The code used for responding
        /// </summary>
        private readonly string m_code;

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.OutOfBandAuthenticationHandler"/> class.
        /// </summary>
        /// <param name="authenticationCode">The shared secret.</param>
        public OutOfBandClientOnlyAuthenticationHandler(string authenticationCode)
        {
            m_code = authenticationCode;
        }

        /// <summary>
        /// Creates a request code
        /// </summary>
        /// <returns>The request code.</returns>
        public string CreateRequest()
        {
            return m_code;
        }

        /// <summary>
        /// Creates the response to a request.
        /// </summary>
        /// <returns>The response code.</returns>
        /// <param name="request">The request code.</param>
        public string CreateResponse(string request)
        {
            return null;
        }

        /// <summary>
        /// Validates the response.
        /// </summary>
        /// <returns><c>true</c>, if response was validated, <c>false</c> otherwise.</returns>
        /// <param name="request">The initial request message</param>
        /// <param name="response">The response to validate.</param>
        public bool ValidateResponse(string request, string response)
        {
            return true;
        }

        /// <summary>
        /// Validates the request.
        /// </summary>
        /// <returns><c>true</c>, if response was validated, <c>false</c> otherwise.</returns>
        /// <param name="request">The response to validate.</param>
        public bool ValidateRequest(string request)
        {
            return string.Equals(request, m_code);
        }
    }

    /// <summary>
    /// Authentication handler stub that disables authentication
    /// </summary>
    public class NoAuthenticationHandler : IAuthenticationHandler
    {
        /// <summary>
        /// Creates a request code
        /// </summary>
        /// <returns>The request code.</returns>
        public string CreateRequest()
        {
            return null;
        }

        /// <summary>
        /// Creates the response to a request.
        /// </summary>
        /// <returns>The response code.</returns>
        /// <param name="request">The request code.</param>
        public string CreateResponse(string request)
        {
            return null;
        }

        /// <summary>
        /// Validates the response.
        /// </summary>
        /// <returns><c>true</c>, if response was validated, <c>false</c> otherwise.</returns>
        /// <param name="request">The initial request message</param>
        /// <param name="response">The response to validate.</param>
        public bool ValidateResponse(string request, string response)
        {
            return true;
        }

        /// <summary>
        /// Validates the request.
        /// </summary>
        /// <returns><c>true</c>, if response was validated, <c>false</c> otherwise.</returns>
        /// <param name="request">The response to validate.</param>
        public bool ValidateRequest(string request)
        {
            return true;
        }
    }
}
