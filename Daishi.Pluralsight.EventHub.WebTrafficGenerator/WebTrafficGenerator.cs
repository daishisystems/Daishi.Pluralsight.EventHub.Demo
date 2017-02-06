using System;
using System.Threading.Tasks;

namespace Daishi.Pluralsight.EventHub.WebTrafficGenerator
{
    internal class WebTrafficGenerator
    {
        public static void Start(
            string eventHubConnectionString,
            int numSimulatedHttpRequests)
        {
            EventHubToolbox.Instance.Connect(eventHubConnectionString);
            var random = new Random();

            for (var i = 0; i < numSimulatedHttpRequests; i++)
            {
                var webTraffic = WebTrafficEvent.GenerateRandom(random);
                // todo: Implement and leverage SendBatch/UnSubscribe(id).
            }
        }

        public static async Task StartAsync()
        {
        }
    }
}