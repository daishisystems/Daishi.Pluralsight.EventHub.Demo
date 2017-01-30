using System;

namespace Daishi.Pluralsight.EventHub
{
    /// <summary>
    ///     <see cref="EventHubToolboxException" /> augments <see cref="Exception" />
    ///     objects with information specific to the particular
    ///     <see cref="Exception" /> that occurred.
    ///     <see cref="EventHubToolboxException" /> also allows components that
    ///     leverage <see cref="EventHubToolbox" /> to catch <see cref="Exception" />s
    ///     specific to <see cref="EventHubToolbox" /> functionality.
    /// </summary>
    [Serializable]
    public class EventHubToolboxException : Exception
    {
        public EventHubToolboxException()
        {
        }

        public EventHubToolboxException(string message)
            : base(message)
        {
        }

        public EventHubToolboxException(
            string message,
            Exception inner)
            : base(message, inner)
        {
        }
    }
}