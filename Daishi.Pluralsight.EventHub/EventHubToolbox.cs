using System;
using System.Text;
using Microsoft.ServiceBus.Messaging;

namespace Daishi.Pluralsight.EventHub
{
    /// <summary>
    ///     <see cref="EventHubToolbox" /> is a collection of functions that provide an
    ///     interface with Event Hub components in Microsoft Azure.
    /// </summary>
    public class EventHubToolbox
    {
        /// <summary>
        ///     <see cref="Connect" /> establishes a connection to an Event Hub instance
        ///     specified by <see cref="connectionString" />.
        /// </summary>
        /// <param name="connectionString">
        ///     <see cref="connectionString" /> is the Event Hub
        ///     connection-string.
        /// </param>
        /// <param name="eventHubClient">
        ///     <see cref="eventHubClient" /> is an instance of
        ///     <see cref="EventHubClient" /> that retains a connection to an Event Hub
        ///     instance.
        /// </param>
        /// <returns><c>true</c>, on successful connection. Otherwise, <c>false</c>.</returns>
        /// <exception cref="ArgumentNullException">
        ///     <see cref="ArgumentNullException" /> is thrown when
        ///     <see cref="connectionString" /> is invalid.
        /// </exception>
        /// <exception cref="EventHubToolboxException">
        ///     <see cref="EventHubToolboxException" /> is thrown when a connection to
        ///     an Event Hub instance cannot be established.
        /// </exception>
        public static bool Connect(
            string connectionString,
            out EventHubClient eventHubClient)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentNullException(nameof(connectionString));
            }
            try
            {
                eventHubClient = EventHubClient.CreateFromConnectionString(
                    connectionString);
                return !eventHubClient.IsClosed;
            }
            catch (Exception exception)
            {
                throw new EventHubToolboxException(
                    ErrorMessageResources.ConnectionFailed,
                    exception);
            }
        }

        /// <summary>
        ///     <see cref="Send" /> publishes <see cref="message" /> to an Event Hub
        ///     instance managed by <see cref="eventHubClient" />.
        /// </summary>
        /// <param name="message">
        ///     <see cref="message" /> is the message that will be
        ///     published to an Event Hub instance managed by <see cref="eventHubClient" />
        ///     .
        /// </param>
        /// <param name="eventHubClient">
        ///     <see cref="eventHubClient" /> is an instance of
        ///     <see cref="EventHubClient" /> that retains a connection to an Event Hub
        ///     instance.
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///     <see cref="ArgumentNullException" /> is
        ///     thrown when <see cref="message" /> is invalid.
        /// </exception>
        /// <exception cref="ArgumentException">
        ///     <see cref="ArgumentException" /> is thrown
        ///     when <see cref="eventHubClient" /> does not retain a valid connection to
        ///     Event Hub.
        /// </exception>
        /// <exception cref="EventHubToolboxException">
        ///     <see cref="EventHubToolboxException" /> is thrown when
        ///     <see cref="message" /> cannot be published to an Event Hub instance.
        /// </exception>
        public void Send(
            string message,
            EventHubClient eventHubClient)
        {
            if (string.IsNullOrEmpty(message))
            {
                throw new ArgumentNullException(nameof(message));
            }
            if (eventHubClient == null || eventHubClient.IsClosed)
            {
                throw new ArgumentException(
                    ErrorMessageResources.Uninitialized,
                    nameof(eventHubClient));
            }
            try
            {
                eventHubClient.Send(
                    new EventData(Encoding.UTF8.GetBytes(message)));
            }
            catch (Exception exception)
            {
                throw new EventHubToolboxException(
                    ErrorMessageResources.UnableToSendMessage,
                    exception);
            }
        }

        /// <summary>
        ///     <see cref="SendAsync" /> asynchronously publishes <see cref="message" /> to
        ///     an Event Hub
        ///     instance managed by <see cref="eventHubClient" />.
        /// </summary>
        /// <param name="message">
        ///     <see cref="message" /> is the message that will be
        ///     published to an Event Hub instance managed by <see cref="eventHubClient" />
        ///     .
        /// </param>
        /// <param name="eventHubClient">
        ///     <see cref="eventHubClient" /> is an instance of
        ///     <see cref="EventHubClient" /> that retains a connection to an Event Hub
        ///     instance.
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///     <see cref="ArgumentNullException" /> is
        ///     thrown when <see cref="message" /> is invalid.
        /// </exception>
        /// <exception cref="ArgumentException">
        ///     <see cref="ArgumentException" /> is thrown
        ///     when <see cref="eventHubClient" /> does not retain a valid connection to
        ///     Event Hub.
        /// </exception>
        /// <exception cref="EventHubToolboxException">
        ///     <see cref="EventHubToolboxException" /> is thrown when
        ///     <see cref="message" /> cannot be published to an Event Hub instance.
        /// </exception>
        public async void SendAsync(
            string message,
            EventHubClient eventHubClient)
        {
            if (string.IsNullOrEmpty(message))
            {
                throw new ArgumentNullException(nameof(message));
            }
            if (eventHubClient == null || eventHubClient.IsClosed)
            {
                throw new ArgumentException(
                    ErrorMessageResources.Uninitialized,
                    nameof(eventHubClient));
            }
            try
            {
                await eventHubClient.SendAsync(
                    new EventData(Encoding.UTF8.GetBytes(message)));
            }
            catch (Exception exception)
            {
                throw new EventHubToolboxException(
                    ErrorMessageResources.UnableToSendMessage,
                    exception);
            }
        }
    }
}