#region Includes

using System;
using System.Threading.Tasks;
using Newtonsoft.Json;

#endregion

namespace Daishi.Pluralsight.EventHub.WebTrafficGenerator {
    /// <summary>
    ///     <see cref="WebTrafficGenerator" /> instantiates
    ///     <see cref="DeviceTelemetry" /> instances, and publishes them to Event
    ///     Hub.
    /// </summary>
    internal sealed class WebTrafficGenerator {
        /// <summary>
        ///     <see cref="SimulatedHttpRequestPublishedEventHandler" /> is a
        ///     function-pointer that facilitates
        ///     <see cref="WebTrafficGenerator.SimulatedHttpRequestPublished" />.
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="e"></param>
        public delegate void SimulatedHttpRequestPublishedEventHandler(
            object sender,
            SimulatedHttpRequestPublishedEventArgs e);

        /// <summary>
        ///     <see cref="SimulatedHttpRequestPublished" /> is raised when a
        ///     <see cref="DeviceTelemetry" /> is published to Event Hub.
        /// </summary>
        public event SimulatedHttpRequestPublishedEventHandler SimulatedHttpRequestPublished;

        /// <summary>
        ///     <see cref="Generate" /> instantiates
        ///     <see cref="numSimulatedHttpRequests" /> <see cref="DeviceTelemetry" />
        ///     instances, and subsequently publishes them to Event Hub.
        /// </summary>
        /// <param name="eventHubConnectionString">
        ///     <see cref="eventHubConnectionString" />
        ///     is the connection-string that facilitates connectivity to an Event Hub.
        /// </param>
        /// <param name="numSimulatedHttpRequests">
        ///     <see cref="numSimulatedHttpRequests" />
        ///     is the number of <see cref="DeviceTelemetry" /> instances to
        ///     instantiate and publish to Event Hub.
        /// </param>
        public void Generate(
            string eventHubConnectionString,
            int numSimulatedHttpRequests) {
            EventHubToolbox.Instance.Connect(eventHubConnectionString);
            var random = new Random();

            for (var i = 0; i < numSimulatedHttpRequests; i++) {
                var webTrafficEvent = DeviceTelemetry.GenerateRandom(random);
                var eventPayload = JsonConvert.SerializeObject(webTrafficEvent);

                EventHubToolbox.Instance.Send(eventPayload);
                OnSimulatedHttpRequestPublished(new SimulatedHttpRequestPublishedEventArgs {
                    IpAddress = webTrafficEvent.IpAddress,
                    NumSimulatedHttpRequests = i + 1
                });
            }
        }

        /// <summary>
        ///     <see cref="GenerateAsync" /> asynchronously instantiates
        ///     <see cref="numSimulatedHttpRequests" /> <see cref="DeviceTelemetry" />
        ///     instances, and subsequently publishes them to Event Hub.
        /// </summary>
        /// <param name="eventHubConnectionString">
        ///     <see cref="eventHubConnectionString" />
        ///     is the connection-string that facilitates connectivity to an Event Hub.
        /// </param>
        /// <param name="numSimulatedHttpRequests">
        ///     <see cref="numSimulatedHttpRequests" />
        ///     is the number of <see cref="DeviceTelemetry" /> instances to
        ///     instantiate and publish to Event Hub.
        /// </param>
        public async Task GenerateAsync(
            string eventHubConnectionString,
            int numSimulatedHttpRequests) {
            EventHubToolbox.Instance.Connect(eventHubConnectionString);
            var random = new Random();

            for (var i = 0; i < numSimulatedHttpRequests; i++) {
                var webTrafficEvent = DeviceTelemetry.GenerateRandom(random);
                var eventPayload = JsonConvert.SerializeObject(webTrafficEvent);

                await EventHubToolbox.Instance.SendAsync(eventPayload);
            }
        }

        /// <summary>
        ///     <see cref="OnSimulatedHttpRequestPublished" /> invokes any subscribers to
        ///     <see cref="SimulatedHttpRequestPublished" />.
        /// </summary>
        /// <param name="e">
        ///     <see cref="e" /> is the
        ///     <see cref="SimulatedHttpRequestPublishedEventArgs" /> instance containing
        ///     metadata
        ///     pertaining to the
        ///     <see cref="WebTrafficGenerator.SimulatedHttpRequestPublished" /> event.
        /// </param>
        private void OnSimulatedHttpRequestPublished(
            SimulatedHttpRequestPublishedEventArgs e) {
            SimulatedHttpRequestPublished?.Invoke(this, e);
        }
    }
}