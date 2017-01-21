using System;
using System.Configuration;
using Microsoft.ServiceBus.Messaging;

namespace Daishi.Pluralsight.EventHub.ConsoleApp
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            Console.ForegroundColor = ConsoleColor.Blue;

            EventHubClient eventHubClient;
            var eventHubConnectionString = ConfigurationManager.AppSettings["EventHubConnectionString"];

            var isConnected = EventHubToolbox.Connect(
                eventHubConnectionString,
                out eventHubClient);

            if (isConnected)
            {
                Console.WriteLine(@"Connected OK!");
            }
            Console.ReadLine();
            eventHubClient.Close();
        }
    }
}