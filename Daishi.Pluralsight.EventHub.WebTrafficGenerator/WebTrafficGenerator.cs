using System;
using System.Threading.Tasks;
using Newtonsoft.Json;

namespace Daishi.Pluralsight.EventHub.WebTrafficGenerator
{
    /// <summary>
    ///     <see cref="WebTrafficGenerator" /> instantiates
    ///     <see cref="SimulatedHttpRequest" /> instances, and publishes them to an
    ///     Event
    ///     Hub.
    /// </summary>
    internal class WebTrafficGenerator
    {
        /// <summary>
        ///     <see cref="Generate" /> instantiates
        ///     <see cref="numSimulatedHttpRequests" /> <see cref="SimulatedHttpRequest" />
        ///     instances, and subsequently publishes them to Event Hub.
        /// </summary>
        /// <param name="eventHubConnectionString">
        ///     <see cref="eventHubConnectionString" />
        ///     is the connection-string that facilitates connectivity to an Event Hub.
        /// </param>
        /// <param name="numSimulatedHttpRequests">
        ///     <see cref="numSimulatedHttpRequests" />
        ///     is the number of <see cref="SimulatedHttpRequest" /> instances to
        ///     instantiate and publish to Event Hub.
        /// </param>
        public static void Generate(
            string eventHubConnectionString,
            int numSimulatedHttpRequests)
        {
            EventHubToolbox.Instance.Connect(eventHubConnectionString);
            var random = new Random();

            for (var i = 0; i < numSimulatedHttpRequests; i++)
            {
                var webTrafficEvent = SimulatedHttpRequest.GenerateRandom(random);
                var eventPayload = JsonConvert.SerializeObject(webTrafficEvent);

                EventHubToolbox.Instance.Send(eventPayload);
            }
        }

        /// <summary>
        ///     <see cref="GenerateAsync" /> asynchronously instantiates
        ///     <see cref="numSimulatedHttpRequests" /> <see cref="SimulatedHttpRequest" />
        ///     instances, and subsequently publishes them to Event Hub.
        /// </summary>
        /// <param name="eventHubConnectionString">
        ///     <see cref="eventHubConnectionString" />
        ///     is the connection-string that facilitates connectivity to an Event Hub.
        /// </param>
        /// <param name="numSimulatedHttpRequests">
        ///     <see cref="numSimulatedHttpRequests" />
        ///     is the number of <see cref="SimulatedHttpRequest" /> instances to
        ///     instantiate and publish to Event Hub.
        /// </param>
        public static async Task GenerateAsync(
            string eventHubConnectionString,
            int numSimulatedHttpRequests)
        {
            EventHubToolbox.Instance.Connect(eventHubConnectionString);
            var random = new Random();

            for (var i = 0; i < numSimulatedHttpRequests; i++)
            {
                var webTrafficEvent = SimulatedHttpRequest.GenerateRandom(random);
                var eventPayload = JsonConvert.SerializeObject(webTrafficEvent);

                await EventHubToolbox.Instance.SendAsync(eventPayload);
            }
        }
    }
}