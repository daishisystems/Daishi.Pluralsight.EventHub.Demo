using System;

namespace Daishi.Pluralsight.EventHub
{
    /// <summary>
    ///     <see cref="NotificationReceivedEventArgs" /> instances are marshaled
    ///     through
    ///     events invoked by <see cref="EventReceiver.NotificationReceived" />.
    /// </summary>
    public class NotificationReceivedEventArgs : EventArgs
    {
        /// <summary>
        ///     <see cref="Notificaction" /> is a notification message pertaining to an
        ///     operational event of interest that occurs during an
        ///     <see cref="EventReceiver" /> instance' life-cycle.
        /// </summary>
        public string Notificaction
        {
            get;
            set;
        }

        /// <summary>
        ///     <see cref="PartitionId" /> is the Event Hub Partition ID pertaining to the
        ///     notification.
        /// </summary>
        public string PartitionId
        {
            get;
            set;
        }

        /// <summary>
        ///     <see cref="NotificationSource" /> is the source operation from which a
        ///     notification occurs.
        /// </summary>
        public NotificationSource NotificationSource
        {
            get;
            set;
        }
    }
}