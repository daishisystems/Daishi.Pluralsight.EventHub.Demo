#region Includes

using System;
using Microsoft.ServiceBus.Messaging;

#endregion

namespace Daishi.Pluralsight.EventHub {
    /// <summary>
    ///     <see cref="NotificationSource" /> represents the source operation from
    ///     which a notification occurs.
    /// </summary>
    [Flags]
    public enum NotificationSource {
        /// <summary>
        ///     <see cref="None" /> is the default value.
        /// </summary>
        None = 0,

        /// <summary>
        ///     <see cref="Close" /> refers to <see cref="IEventProcessor.CloseAsync" />.
        /// </summary>
        Close = 1 << 0,

        /// <summary>
        ///     <see cref="Open" /> refers to <see cref="IEventProcessor.OpenAsync" />.
        /// </summary>
        Open = 1 << 1,

        /// <summary>
        ///     <see cref="ProcessEvents" /> refers to
        ///     <see cref="IEventProcessor.ProcessEventsAsync" />.
        /// </summary>
        ProcessEvents = 1 << 2
    }
}