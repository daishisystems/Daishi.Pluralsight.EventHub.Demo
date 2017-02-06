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

        private EventHubClient _eventHubClient;

        private EventHubToolbox()
        {
            EventProcessorHosts = new Dictionary<string, EventProcessorHost>();
        }

        public static EventHubToolbox Instance => Lazy.Value;

        public bool IsSubscribedToAny => EventProcessorHosts != null
                                         && EventProcessorHosts.Count > 0;

        public bool IsConnected => _eventHubClient != null && !_eventHubClient.IsClosed;

        public Dictionary<string, EventProcessorHost> EventProcessorHosts
        {
            get;
        }

        /// <summary>
        ///     <see cref="Connect" /> establishes a connection to an Event Hub instance
        ///     specified by <see cref="connectionString" />.
        /// </summary>
        /// <param name="connectionString">
        ///     <see cref="connectionString" /> is the Event Hub
        ///     connection-string.
        /// </param>
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
        ///     <see cref="Send" /> publishes <see cref="message" /> to an Event Hub, if
        ///     connected.
        /// </summary>
        /// <param name="message">
        ///     <see cref="message" /> is the message that will be published to the Event
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
        ///     <see cref="SendAsync" /> asynchronously publishes <see cref="message" /> to
        ///     an
        ///     Event Hub, if connected.
        /// </summary>
        /// <param name="message">
        ///     <see cref="message" /> is the message that will be published to the Event
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
        ///     <see cref="Subscribe" /> asynchronously acquires a lease on an Event
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
        /// <param name="registerAction">
        ///     <see cref="registerAction" /> is a function that
        ///     abstracts the <see cref="EventProcessorHost" /> registration process
        ///     outside
        ///     of this method, in order to facilitate unit-testing.
        /// </param>
        /// <param name="testMode">
        ///     <see cref="testMode" /> determines whether or not this
        ///     function is invoked from a unit-test, or not.
        /// </param>
        /// <param name="eventProcessorOptions">
        ///     <see cref="EventProcessorOptions" /> allows custom <see cref="Exception" />
        ///     -handling, among other things, when interfacing with the Event hub
        ///     partition.
        /// </param>
        /// <param name="unRegisterAction">
        ///     <see cref="unRegisterAction" /> is a function that
        ///     abstracts the <see cref="EventProcessorHost" /> un-register process outside
        ///     of this method, in order to facilitate unit-testing.
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///     <see cref="ArgumentNullException" /> is
        ///     thrown when input metadata pertaining to an Event Hub (e.g.,
        ///     <see cref="hostName" />/<see cref="eventHubConnectionString" />) is
        ///     invalid.
        /// </exception>
        /// <exception cref="EventHubToolboxException">
        ///     <see cref="EventHubToolboxException" /> is thrown when an
        ///     <see cref="Exception" /> occurs while subscribing to
        ///     <see cref="eventHubName" />, or when un-subscribing from an existing
        ///     <see cref="EventProcessorHost" /> instance.
        /// </exception>
        /// <remarks>
        ///     Added <see cref="testMode" /> in order to swallow
        ///     <see cref="EventProcessorHost" />-initialisation errors during unit-test
        ///     invocation. Instantiating <see cref="EventProcessorHost" /> requires a
        ///     valid Azure Storage Account, and as such, the method cannot be abstracted
        ///     to return, for example, a default instance of
        ///     <see cref="EventProcessorHost" />.
        /// </remarks>
        public void Subscribe(
            string hostName,
            string eventHubConnectionString,
            string eventHubName,
            string storageAccountName,
            string storageAccountKey,
            IEventProcessor eventProcessor,
            Action<EventProcessorHost> unRegisterAction,
            Action<EventProcessorHost,
                BridgeEventProcessorFactory,
                EventProcessorOptions> registerAction,
            bool testMode = false,
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
            if (EventProcessorHosts.ContainsKey(hostName))
            {
                EventProcessorHost existingEventProcessorHost = null;
                try
                {
                    existingEventProcessorHost = EventProcessorHosts[hostName];
                    unRegisterAction(existingEventProcessorHost);
                }
                catch (Exception exception)
                {
                    var eventHubHostName = existingEventProcessorHost != null
                        ? existingEventProcessorHost.HostName
                        : "Unknown";
                    throw new EventHubToolboxException($"Error un-subscribing from {eventHubHostName}.",
                        exception);
                }
            }
            try
            {
                var storageConnectionString =
                    $"DefaultEndpointsProtocol=https;AccountName={storageAccountName}" +
                    $";AccountKey={storageAccountKey}";

                EventProcessorHost eventProcessorHost = null;
                try
                {
                    eventProcessorHost = new EventProcessorHost(
                        hostName,
                        eventHubName,
                        EventHubConsumerGroup.DefaultGroupName,
                        eventHubConnectionString,
                        storageConnectionString);
                }
                catch (Exception)
                {
                    if (!testMode)
                    {
                        throw;
                    }
                }
                var factory = new BridgeEventProcessorFactory(eventProcessor);

                registerAction(eventProcessorHost, factory, eventProcessorOptions);
                EventProcessorHosts.Add(hostName, eventProcessorHost);
            }
            catch (Exception exception)
            {
                throw new EventHubToolboxException($"Error subscribing to {eventHubName}.",
                    exception);
            }
        }

        /// <summary>
        ///     <see cref="SubscribeAsync" /> asynchronously acquires a lease on an Event
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
        /// <param name="registerFunc">
        ///     <see cref="registerFunc" /> is a function that
        ///     abstracts the <see cref="EventProcessorHost" /> registration process
        ///     outside
        ///     of this method, in order to facilitate unit-testing.
        /// </param>
        /// <param name="testMode">
        ///     <see cref="testMode" /> determines whether or not this
        ///     function is invoked from a unit-test, or not.
        /// </param>
        /// <param name="eventProcessorOptions">
        ///     <see cref="EventProcessorOptions" /> allows custom <see cref="Exception" />
        ///     -handling, among other things, when interfacing with the Event hub
        ///     partition.
        /// </param>
        /// <param name="unRegisterFunc">
        ///     <see cref="unRegisterFunc" /> is a function that
        ///     abstracts the <see cref="EventProcessorHost" /> un-register process outside
        ///     of this method, in order to facilitate unit-testing.
        /// </param>
        /// <exception cref="ArgumentNullException">
        ///     <see cref="ArgumentNullException" /> is
        ///     thrown when input metadata pertaining to an Event Hub (e.g.,
        ///     <see cref="hostName" />/<see cref="eventHubConnectionString" />) is
        ///     invalid.
        /// </exception>
        /// <exception cref="EventHubToolboxException">
        ///     <see cref="EventHubToolboxException" /> is thrown when an
        ///     <see cref="Exception" /> occurs while subscribing to
        ///     <see cref="eventHubName" />, or when un-subscribing from an existing
        ///     <see cref="EventProcessorHost" /> instance.
        /// </exception>
        public async Task SubscribeAsync(
            string hostName,
            string eventHubConnectionString,
            string eventHubName,
            string storageAccountName,
            string storageAccountKey,
            IEventProcessor eventProcessor,
            Func<EventProcessorHost,
                Task> unRegisterFunc,
            Func<EventProcessorHost,
                BridgeEventProcessorFactory,
                EventProcessorOptions,
                Task> registerFunc,
            bool testMode = false,
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
            if (EventProcessorHosts.ContainsKey(hostName))
            {
                EventProcessorHost existingEventProcessorHost = null;
                try
                {
                    existingEventProcessorHost = EventProcessorHosts[hostName];
                    await unRegisterFunc(existingEventProcessorHost);
                }
                catch (Exception exception)
                {
                    var eventHubHostName = existingEventProcessorHost != null
                        ? existingEventProcessorHost.HostName
                        : "Unknown";
                    throw new EventHubToolboxException($"Error un-subscribing from {eventHubHostName}.",
                        exception);
                }
            }
            try
            {
                var storageConnectionString =
                    $"DefaultEndpointsProtocol=https;AccountName={storageAccountName}" +
                    $";AccountKey={storageAccountKey}";

                EventProcessorHost eventProcessorHost = null;
                try
                {
                    eventProcessorHost = new EventProcessorHost(
                        hostName,
                        eventHubName,
                        EventHubConsumerGroup.DefaultGroupName,
                        eventHubConnectionString,
                        storageConnectionString);
                }
                catch (Exception)
                {
                    if (!testMode)
                    {
                        throw;
                    }
                }
                var factory = new BridgeEventProcessorFactory(eventProcessor);

                await registerFunc(eventProcessorHost, factory, eventProcessorOptions);
                EventProcessorHosts.Add(hostName, eventProcessorHost);
            }
            catch (Exception exception)
            {
                throw new EventHubToolboxException($"Error subscribing to {eventHubName}.",
                    exception);
            }
        }

        /// <summary>
        ///     <see cref="UnsubscribeAll" /> resets the lease on all
        ///     currently subscribed-to Event Hub partition(s), if any.
        /// </summary>
        /// <param name="unregisterAction">
        ///     <see cref="unregisterAction" /> is a function that
        ///     abstracts the <see cref="EventProcessorHost" /> un-register process outside
        ///     of this method, in order to facilitate unit-testing.
        /// </param>
        public void UnsubscribeAll(Action<EventProcessorHost> unregisterAction)
        {
            if (!IsSubscribedToAny) return;
            var unregisteredHostNames = new List<string>();

            foreach (var hostName in EventProcessorHosts.Keys)
            {
                try
                {
                    var eventProcessorHost = EventProcessorHosts[hostName];
                    unregisterAction(eventProcessorHost);
                    unregisteredHostNames.Add(hostName);
                }
                catch (Exception exception)
                {
                    throw new EventHubToolboxException(
                        $"Unable to un-subscribe from {hostName}.", exception);
                }
            }
            foreach (var hostName in unregisteredHostNames)
            {
                EventProcessorHosts.Remove(hostName);
            }
        }

        /// <summary>
        ///     <see cref="UnsubscribeAllAsync" /> asynchronously resets the lease on all
        ///     currently subscribed-to Event Hub partition(s), if any.
        /// </summary>
        /// <param name="unregisterFunc">
        ///     <see cref="unregisterFunc" /> is a function that
        ///     abstracts the <see cref="EventProcessorHost" /> un-register process outside
        ///     of this method, in order to facilitate unit-testing.
        /// </param>
        public async Task UnsubscribeAllAsync(Func<EventProcessorHost, Task> unregisterFunc)
        {
            if (IsSubscribedToAny)
            {
                var unregisteredHostNames = new List<string>();
                foreach (var hostName in EventProcessorHosts.Keys)
                {
                    try
                    {
                        var eventProcessorHost = EventProcessorHosts[hostName];
                        await unregisterFunc(eventProcessorHost);
                        unregisteredHostNames.Add(hostName);
                    }
                    catch (Exception exception)
                    {
                        throw new EventHubToolboxException(
                            $"Unable to un-subscribe from {hostName}.", exception);
                    }
                }
                foreach (var hostName in unregisteredHostNames)
                {
                    EventProcessorHosts.Remove(hostName);
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
            return EventProcessorHosts != null
                   && EventProcessorHosts.ContainsKey(hostName);
        }

        /// <summary>
        ///     <see cref="Register" /> registers
        ///     <see cref="bridgeEventProcessorFactory" /> with
        ///     <see cref="eventProcessorHost" /> and <see cref="eventProcessorOptions" />,
        ///     if applicable.
        /// </summary>
        /// <param name="eventProcessorHost">
        ///     <see cref="eventProcessorHost" /> is the
        ///     <see cref="EventProcessorHost" /> instance to register.
        /// </param>
        /// <param name="bridgeEventProcessorFactory">
        ///     <see cref="bridgeEventProcessorFactory" /> is the
        ///     <see cref="BridgeEventProcessorFactory" /> instance to register.
        /// </param>
        /// <param name="eventProcessorOptions">
        ///     <see cref="eventProcessorOptions" /> is the
        ///     <see cref="EventProcessorOptions" /> instance to register, if applicable.
        /// </param>
        public static void Register(
            EventProcessorHost eventProcessorHost,
            BridgeEventProcessorFactory bridgeEventProcessorFactory,
            EventProcessorOptions eventProcessorOptions = null)
        {
            if (eventProcessorOptions == null)
            {
                eventProcessorHost.RegisterEventProcessorFactoryAsync(
                    bridgeEventProcessorFactory).Wait();
            }
            else
            {
                eventProcessorHost.RegisterEventProcessorFactoryAsync(
                    bridgeEventProcessorFactory,
                    eventProcessorOptions).Wait();
            }
        }

        /// <summary>
        ///     <see cref="RegisterAsync" /> asynchronously registers
        ///     <see cref="bridgeEventProcessorFactory" /> with
        ///     <see cref="eventProcessorHost" /> and <see cref="eventProcessorOptions" />,
        ///     if applicable.
        /// </summary>
        /// <param name="eventProcessorHost">
        ///     <see cref="eventProcessorHost" /> is the
        ///     <see cref="EventProcessorHost" /> instance to register.
        /// </param>
        /// <param name="bridgeEventProcessorFactory">
        ///     <see cref="bridgeEventProcessorFactory" /> is the
        ///     <see cref="BridgeEventProcessorFactory" /> instance to register.
        /// </param>
        /// <param name="eventProcessorOptions">
        ///     <see cref="eventProcessorOptions" /> is the
        ///     <see cref="EventProcessorOptions" /> instance to register, if applicable.
        /// </param>
        public static async Task RegisterAsync(
            EventProcessorHost eventProcessorHost,
            BridgeEventProcessorFactory bridgeEventProcessorFactory,
            EventProcessorOptions eventProcessorOptions = null)
        {
            if (eventProcessorOptions == null)
            {
                await eventProcessorHost.RegisterEventProcessorFactoryAsync(
                    bridgeEventProcessorFactory);
            }
            else
            {
                await eventProcessorHost.RegisterEventProcessorFactoryAsync(
                    bridgeEventProcessorFactory,
                    eventProcessorOptions);
            }
        }

        /// <summary>
        ///     <see cref="UnRegister" /> un-registers <see cref="EventProcessorHost" />,
        ///     shutting down the instance, and allowing the associated Event Hub
        ///     partition-lease to be released gracefully.
        /// </summary>
        /// <param name="eventProcessorHost">
        ///     <see cref="eventProcessorHost" /> is the
        ///     <see cref="EventProcessorHost" /> instance to be un-registered.
        /// </param>
        /// <remarks>This is a helper function, designed to facilitate unit-testing.</remarks>
        public static void UnRegister(EventProcessorHost eventProcessorHost)
        {
            eventProcessorHost.UnregisterEventProcessorAsync().Wait();
        }

        /// <summary>
        ///     <see cref="UnRegisterAsync" /> asynchronously un-registers
        ///     <see cref="EventProcessorHost" />,
        ///     shutting down the instance, and allowing the associated Event Hub
        ///     partition-lease to be released gracefully.
        /// </summary>
        /// <param name="eventProcessorHost">
        ///     <see cref="eventProcessorHost" /> is the
        ///     <see cref="EventProcessorHost" /> instance to be un-registered.
        /// </param>
        /// <remarks>This is a helper function, designed to facilitate unit-testing.</remarks>
        public static async Task UnRegisterAsync(EventProcessorHost eventProcessorHost)
        {
            await eventProcessorHost.UnregisterEventProcessorAsync();
        }
    }
}