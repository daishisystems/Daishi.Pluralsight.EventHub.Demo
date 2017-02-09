using System;

namespace Daishi.Pluralsight.EventHub
{
    /// <summary>
    ///     <see cref="EventReceivedEventArgs" /> instances are marshaled through
    ///     events invoked by <see cref="EventReceiver.EventReceived" />.
    /// </summary>
    public class EventReceivedEventArgs : EventArgs
    {
        /// <summary>
        ///     <see cref="Event" /> is a UTF8-encoded string-representation of an Event
        ///     Hub event.
        /// </summary>
        public string Event
        {
            get;
            set;
        }

        /// <summary>
        ///     <see cref="PartitionId" /> is the Event Hub Partition ID, through which the
        ///     event was published.
        /// </summary>
        public string PartitionId
        {
            get;
            set;
        }
    }
}