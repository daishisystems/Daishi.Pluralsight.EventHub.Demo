using System;
using System.Collections.Generic;
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

        private readonly Dictionary<string, EventProcessorHost> _eventProcessorHosts;

        private EventHubClient _eventHubClient;

        private EventHubToolbox()
        {
            _eventProcessorHosts = new Dictionary<string, EventProcessorHost>();
        }

        public static EventHubToolbox Instance => Lazy.Value;

        public bool IsSubscribedToAny => _eventProcessorHosts.Count > 0;

        public bool IsConnected => _eventHubClient != null && !_eventHubClient.IsClosed;

        /// <summary>
        ///     <see cref="Connect" /> establishes a connection to an Event Hub instance
        ///     specified by <see cref="connectionString" />.
        /// </summary>
        /// <param name="connectionString">
        ///     <see cref="connectionString" /> is the Event Hub
        ///     connection-string.
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
        public void Connect(string connectionString)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new ArgumentNullException(nameof(connectionString));
            }
            try
            {
                _eventHubClient = EventHubClient.CreateFromConnectionString(
                    connectionString);
            }
            catch (Exception exception)
            {
                throw new EventHubToolboxException(
                    ErrorMessageResources.ConnectionFailed,
                    exception);
            }
        }

        /// <summary>
        ///     <see cref="Send" /> publishes <see cref="message" /> to an Event Hub.
        /// </summary>
        /// <param name="message">
        ///     <see cref="message" /> is the message that will be published to an Event
        ///     Hub.
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///     <see cref="ArgumentNullException" /> is
        ///     thrown when <see cref="message" /> is invalid.
        /// </exception>
        /// <exception cref="ArgumentException">
        ///     <see cref="ArgumentException" /> is thrown when no valid connection to
        ///     Event Hub exists.
        /// </exception>
        /// <exception cref="EventHubToolboxException">
        ///     <see cref="EventHubToolboxException" /> is thrown when
        ///     <see cref="message" /> cannot be published to an Event Hub instance.
        /// </exception>
        public void Send(string message)
        {
            if (string.IsNullOrEmpty(message))
            {
                throw new ArgumentNullException(nameof(message));
            }
            if (_eventHubClient == null || _eventHubClient.IsClosed)
            {
                throw new ArgumentException(
                    ErrorMessageResources.Uninitialized,
                    nameof(_eventHubClient));
            }
            try
            {
                _eventHubClient.Send(
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
        ///     <see cref="Send" /> asynchronously publishes <see cref="message" /> to an
        ///     Event Hub.
        /// </summary>
        /// <param name="message">
        ///     <see cref="message" /> is the message that will be published to an Event
        ///     Hub.
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///     <see cref="ArgumentNullException" /> is
        ///     thrown when <see cref="message" /> is invalid.
        /// </exception>
        /// <exception cref="ArgumentException">
        ///     <see cref="ArgumentException" /> is thrown when no valid connection to
        ///     Event Hub exists.
        /// </exception>
        /// <exception cref="EventHubToolboxException">
        ///     <see cref="EventHubToolboxException" /> is thrown when
        ///     <see cref="message" /> cannot be published to an Event Hub instance.
        /// </exception>
        public async Task SendAsync(string message)
        {
            if (string.IsNullOrEmpty(message))
            {
                throw new ArgumentNullException(nameof(message));
            }
            if (_eventHubClient == null || _eventHubClient.IsClosed)
            {
                throw new ArgumentException(
                    ErrorMessageResources.Uninitialized,
                    nameof(_eventHubClient));
            }
            try
            {
                await _eventHubClient.SendAsync(
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
        ///     <see cref="Subscribe" /> acquires a lease on an Event
        ///     Hub partition, and reads events from the Event Hub as they are published.
        /// </summary>
        /// <param name="hostName">
        ///     <see cref="hostName" /> is a unique identifier assigned
        ///     to a running instance of <see cref="EventProcessorHost" />, created by this
        ///     method.
        /// </param>
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
        public void Subscribe(
            string hostName,
            string eventHubConnectionString,
            string eventHubName,
            string storageAccountName,
            string storageAccountKey,
            IEventProcessor eventProcessor,
            EventProcessorOptions eventProcessorOptions = null)
        {
            if (string.IsNullOrEmpty(hostName))
            {
                throw new ArgumentNullException(nameof(hostName));
            }
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
            if (_eventProcessorHosts.ContainsKey(hostName))
            {
                var existingEventPrpocessorHost = _eventProcessorHosts[hostName];
                existingEventPrpocessorHost.UnregisterEventProcessorAsync().Wait();
            }

            var storageConnectionString =
                $"DefaultEndpointsProtocol=https;AccountName={storageAccountName}" +
                $";AccountKey={storageAccountKey}";

            var eventProcessorHost = new EventProcessorHost(
                hostName,
                eventHubName,
                EventHubConsumerGroup.DefaultGroupName,
                eventHubConnectionString,
                storageConnectionString);

            var factory = new BridgeEventProcessorFactory(eventProcessor);

            if (eventProcessorOptions == null)
            {
                eventProcessorHost
                    .RegisterEventProcessorFactoryAsync(factory)
                    .Wait();
            }
            else
            {
                eventProcessorHost
                    .RegisterEventProcessorFactoryAsync(
                        factory,
                        eventProcessorOptions)
                    .Wait();
            }
            _eventProcessorHosts.Add(hostName, eventProcessorHost);
        }

        /// <summary>
        ///     <see cref="Subscribe" /> acquires a lease on an Event
        ///     Hub partition, and reads events from the Event Hub as they are published.
        /// </summary>
        /// <param name="hostName">
        ///     <see cref="hostName" /> is a unique identifier assigned
        ///     to a running instance of <see cref="EventProcessorHost" />, created by this
        ///     method.
        /// </param>
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
        public async Task SubscribeAsync(
            string hostName,
            string eventHubConnectionString,
            string eventHubName,
            string storageAccountName,
            string storageAccountKey,
            IEventProcessor eventProcessor,
            EventProcessorOptions eventProcessorOptions = null)
        {
            if (string.IsNullOrEmpty(hostName))
            {
                throw new ArgumentNullException(nameof(hostName));
            }
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
            if (_eventProcessorHosts.ContainsKey(hostName))
            {
                var existingEventPrpocessorHost = _eventProcessorHosts[hostName];
                await existingEventPrpocessorHost.UnregisterEventProcessorAsync();
            }

            var storageConnectionString =
                $"DefaultEndpointsProtocol=https;AccountName={storageAccountName}" +
                $";AccountKey={storageAccountKey}";

            var eventProcessorHost = new EventProcessorHost(
                hostName,
                eventHubName,
                EventHubConsumerGroup.DefaultGroupName,
                eventHubConnectionString,
                storageConnectionString);

            var factory = new BridgeEventProcessorFactory(eventProcessor);

            if (eventProcessorOptions == null)
            {
                await eventProcessorHost
                    .RegisterEventProcessorFactoryAsync(factory);
            }
            else
            {
                await eventProcessorHost
                    .RegisterEventProcessorFactoryAsync(
                        factory,
                        eventProcessorOptions);
            }
            _eventProcessorHosts.Add(hostName, eventProcessorHost);
        }

        // todo: CancellationToken, exception-handling.
        // todo: Graphics, animations, blog post.

        /// <summary>
        ///     <see cref="UnsubscribeAll" /> resets the lease on all currently
        ///     subscribed-to Event Hub partition(s), if any.
        /// </summary>
        public void UnsubscribeAll()
        {
            if (!IsSubscribedToAny) return;
            var unregisteredHostNames = new List<string>();

            foreach (var eventProcessorHost in _eventProcessorHosts.Values)
            {
                try
                {
                    eventProcessorHost.UnregisterEventProcessorAsync().Wait();
                    unregisteredHostNames.Add(eventProcessorHost.HostName);
                }
                catch (Exception exception)
                {
                    throw new EventHubToolboxException(
                        $"Unable to un-subscribe from {eventProcessorHost.HostName}.",
                        exception);
                }
            }
            foreach (var hostName in unregisteredHostNames)
            {
                _eventProcessorHosts.Remove(hostName);
            }
        }

        /// <summary>
        ///     <see cref="UnsubscribeAllAsync" /> asynchronously resets the lease on all
        ///     currently subscribed-to Event Hub partition(s), if any.
        /// </summary>
        public async Task UnsubscribeAllAsync()
        {
            if (IsSubscribedToAny)
            {
                var unregisteredHostNames = new List<string>();
                foreach (var eventProcessorHost in _eventProcessorHosts.Values)
                {
                    try
                    {
                        await eventProcessorHost.UnregisterEventProcessorAsync();
                        unregisteredHostNames.Add(eventProcessorHost.HostName);
                    }
                    catch (Exception exception)
                    {
                        throw new EventHubToolboxException(
                            $"Unable to un-subscribe from {eventProcessorHost.HostName}.",
                            exception);
                    }
                }
                foreach (var hostName in unregisteredHostNames)
                {
                    _eventProcessorHosts.Remove(hostName);
                }
            }
        }

        /// <summary>
        ///     <see cref="IsSubscribedTo" /> returns <c>true</c> if
        ///     <see cref="hostName" /> is currently subscribed-to.
        /// </summary>
        /// <param name="hostName">
        ///     <see cref="hostName" /> is the name of the host that is
        ///     potentially subscribed-to.
        /// </param>
        /// <returns>
        ///     Returns <c>true</c> if <see cref="hostName" /> is currently
        ///     subscribed-to.
        /// </returns>
        public bool IsSubscribedTo(string hostName)
        {
            if (string.IsNullOrEmpty(hostName))
            {
                throw new ArgumentNullException(nameof(hostName));
            }
            return _eventProcessorHosts != null
                   && _eventProcessorHosts.ContainsKey(hostName);
        }
    }
}