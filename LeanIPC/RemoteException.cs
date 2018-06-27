using System;
namespace LeanIPC
{
    [Serializable]
    public class RemoteException : Exception
    {
        /// <summary>
        /// The original type of the error
        /// </summary>
        public readonly string ErrorType;

        /// <summary>
        /// Initializes a new instance of the <see cref="T:LeanIPC.RemoteException"/> class.
        /// </summary>
        /// <param name="message">The message to report.</param>
        /// <param name="errortype">The error type to report.</param>
        public RemoteException(string message, string errortype)
            : base(message)
        {
        }
    }
}
