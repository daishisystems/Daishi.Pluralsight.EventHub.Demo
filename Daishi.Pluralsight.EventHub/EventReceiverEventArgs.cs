using System;

namespace Daishi.Pluralsight.EventHub
{
    /// <summary>
    ///     <see cref="EventReceiverEventArgs" /> instances are marshaled through
    ///     events invoked by <see cref="EventReceiver" /> instances.
    /// </summary>
    public class EventReceiverEventArgs : EventArgs
    {
        /// <summary>
        ///     <see cref="Message" /> is either a message-notification, or a UTF8-encoded
        ///     string-representation of an Event Hub event.
        /// </summary>
        public string Message
        {
            get;
            set;
        }
    }
}