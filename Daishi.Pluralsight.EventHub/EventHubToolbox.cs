using System;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace Daishi.Pluralsight.EventHub
{
    /// <summary>
    ///     <see cref="EventHubToolbox" /> is a collection of functions that provide an
    ///     interface with Event Hub components in Microsoft Azure.
    /// </summary>
    public class EventHubToolbox
    {
        private static readonly Lazy<EventHubToolbox> Lazy =
            new Lazy<EventHubToolbox>(() => new EventHubToolbox());

        private EventProcessorHost _eventProcessorHost;

        private EventHubToolbox()
        {
        }

        public static EventHubToolbox Instance => Lazy.Value;

        public bool IsSubscribed
        {
            get;
            private set;
        }

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
        public static void Send(
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
        /// <returns><see cref="Task{TResult}" /> (an empty async response).</returns>
        public static async Task SendAsync(
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

        /// <summary>
        ///     <see cref="Subscribe" /> acquires a lease on an Event Hub partition, and
        ///     reads events from the Event Hub as they are published.
        /// </summary>
        /// <param name="storageConnectionString">
        ///     <see cref="storageConnectionString" /> is
        ///     the Azure Storage connection-string, used to store events as they are
        ///     removed from the Event hub.
        /// </param>
        /// <param name="eventHubConnectionString">
        ///     <see cref="eventHubConnectionString" />
        ///     is the connection-string to the Event Hub.
        /// </param>
        /// <param name="eventHubName">
        ///     <see cref="eventHubName" /> is the name of the Event
        ///     Hub.
        /// </param>
        /// <param name="eventProcessor">
        ///     <see cref="eventProcessor" /> is the
        ///     <see cref="IEventProcessor" /> instance that interfaces with the Event Hub
        ///     partition.
        /// </param>
        /// <param name="eventProcessorOptions">
        ///     <see cref="EventProcessorOptions" /> allows
        ///     custom <see cref="Exception" />-handling, among other things, when
        ///     interfacing with the Event hub partition.
        /// </param>
        public void Subscribe(
            string storageConnectionString,
            string eventHubConnectionString,
            string eventHubName,
            IEventProcessor eventProcessor,
            EventProcessorOptions eventProcessorOptions = null)
        {
            if (string.IsNullOrEmpty(storageConnectionString))
            {
                throw new ArgumentNullException(nameof(storageConnectionString));
            }
            if (string.IsNullOrEmpty(eventHubConnectionString))
            {
                throw new ArgumentNullException(nameof(eventHubConnectionString));
            }
            if (string.IsNullOrEmpty(eventHubName))
            {
                throw new ArgumentNullException(nameof(eventHubName));
            }
            if (eventProcessor == null)
            {
                throw new ArgumentNullException(nameof(eventProcessor));
            }
            if (IsSubscribed)
            {
                _eventProcessorHost?.UnregisterEventProcessorAsync().Wait();
            }

            _eventProcessorHost = new EventProcessorHost(
                Guid.NewGuid().ToString(),
                eventHubName,
                EventHubConsumerGroup.DefaultGroupName,
                eventHubConnectionString,
                storageConnectionString);

            var factory = new BridgeEventProcessorFactory(eventProcessor);

            if (eventProcessorOptions == null)
            {
                _eventProcessorHost
                    .RegisterEventProcessorFactoryAsync(factory)
                    .Wait();
            }
            else
            {
                _eventProcessorHost
                    .RegisterEventProcessorFactoryAsync(
                        factory,
                        eventProcessorOptions)
                    .Wait();
            }
            IsSubscribed = true;
        }

        /// <summary>
        ///     <see cref="SubscribeAsync" /> asynchronously acquires a lease on an Event
        ///     Hub partition, and reads events from the Event Hub as they are published.
        /// </summary>
        /// <param name="eventHubConnectionString">
        ///     <see cref="eventHubConnectionString" /> is the connection-string to the
        ///     Event Hub.
        /// </param>
        /// <param name="eventHubName">
        ///     <see cref="eventHubName" /> is the name of the Event Hub.
        /// </param>
        /// <param name="storageAccountName">
        ///     <see cref="storageAccountName" /> is the name assigned to the Azure storage
        ///     account used to store events as they are removed from the Event hub.
        /// </param>
        /// <param name="storageAccountKey">
        ///     <see cref="storageAccountKey" /> is the access key assigned to the Azure
        ///     storage account used to store events as they are removed from the Event
        ///     hub.
        /// </param>
        /// <param name="eventProcessor">
        ///     <see cref="eventProcessor" /> is the
        ///     <see cref="IEventProcessor" /> instance that interfaces with the Event Hub
        ///     partition.
        /// </param>
        /// <param name="eventProcessorOptions">
        ///     <see cref="EventProcessorOptions" /> allows custom <see cref="Exception" />
        ///     -handling, among other things, when interfacing with the Event hub
        ///     partition.
        /// </param>
        /// <returns><see cref="Task{TResult}" /> (an empty async response).</returns>
        public async Task SubscribeAsync(
            string eventHubConnectionString,
            string eventHubName,
            string storageAccountName,
            string storageAccountKey,
            IEventProcessor eventProcessor,
            EventProcessorOptions eventProcessorOptions = null)
        {
            if (string.IsNullOrEmpty(eventHubConnectionString))
            {
                throw new ArgumentNullException(nameof(eventHubConnectionString));
            }
            if (string.IsNullOrEmpty(eventHubName))
            {
                throw new ArgumentNullException(nameof(eventHubName));
            }
            if (string.IsNullOrEmpty(storageAccountName))
            {
                throw new ArgumentNullException(nameof(storageAccountName));
            }
            if (string.IsNullOrEmpty(storageAccountKey))
            {
                throw new ArgumentNullException(nameof(storageAccountKey));
            }
            if (eventProcessor == null)
            {
                throw new ArgumentNullException(nameof(eventProcessor));
            }
            if (IsSubscribed)
            {
                _eventProcessorHost?.UnregisterEventProcessorAsync().Wait();
            }

            var storageConnectionString =
                $"DefaultEndpointsProtocol=https;AccountName={storageAccountName}" +
                $";AccountKey={storageAccountKey}";

            _eventProcessorHost = new EventProcessorHost(
                Guid.NewGuid().ToString(),
                eventHubName,
                EventHubConsumerGroup.DefaultGroupName,
                eventHubConnectionString,
                storageConnectionString);

            var factory = new BridgeEventProcessorFactory(eventProcessor);

            if (eventProcessorOptions == null)
            {
                await _eventProcessorHost
                    .RegisterEventProcessorFactoryAsync(factory);
            }
            else
            {
                await _eventProcessorHost
                    .RegisterEventProcessorFactoryAsync(
                        factory,
                        eventProcessorOptions);
            }
            IsSubscribed = true;
        }

        /// <summary>
        ///     <see cref="Unsubscribe" /> resets the lease on the currently subscribed-to
        ///     Event Hub partition, if applicable.
        /// </summary>
        public void Unsubscribe()
        {
            if (IsSubscribed)
            {
                _eventProcessorHost?.UnregisterEventProcessorAsync().Wait();
            }
        }

        /// <summary>
        ///     <see cref="UnsubscribeAsync" /> asynchronously resets the lease on the
        ///     currently subscribed-to Event Hub partition, if applicable.
        /// </summary>
        public async Task UnsubscribeAsync()
        {
            if (IsSubscribed && _eventProcessorHost != null)
            {
                await _eventProcessorHost.UnregisterEventProcessorAsync();
            }
        }
    }
}