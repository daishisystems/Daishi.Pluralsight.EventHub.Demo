using System;
using System.Configuration;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;

namespace Daishi.Pluralsight.EventHub.ConsoleApp
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            MainAsync().Wait();
        }

        private static async Task MainAsync()
        {
            Console.ForegroundColor = ConsoleColor.Green;

            EventHubClient eventHubClient;
            var eventHubConnectionString =
                ConfigurationManager.AppSettings["EventHubConnectionString"];

            var isConnected = EventHubToolbox.Connect(
                eventHubConnectionString,
                out eventHubClient);

            if (isConnected)
            {
                Console.WriteLine(@"Connected OK!");
            }

            var eventHubConnectionStringShort =
                ConfigurationManager.AppSettings["EventHubConnectionStringShort"];
            var eventHubName =
                ConfigurationManager.AppSettings["EventHubName"];
            var storageAccountName =
                ConfigurationManager.AppSettings["StorageAccountName"];
            var storageAccountKey =
                ConfigurationManager.AppSettings["StorageAccountKey"];

            var eventReceiver = new EventReceiver();
            eventReceiver.Notification += EventReceiver_Notification;
            eventReceiver.EventReceived += EventReceiverEventReceived;

            var eventProcessorOptions = new EventProcessorOptions();
            eventProcessorOptions.ExceptionReceived += EventProcessorOptions_ExceptionReceived;

            await EventHubToolbox.Instance.SubscribeAsync(
                eventHubConnectionStringShort,
                eventHubName,
                storageAccountName,
                storageAccountKey,
                eventReceiver,
                eventProcessorOptions);

            await EventHubToolbox.SendAsync("TEST", eventHubClient);

            Console.ReadLine();
            await EventHubToolbox.Instance.UnsubscribeAsync();
        }

        private static void EventReceiver_Notification(object sender,
            EventReceiverEventArgs e)
        {
            Console.WriteLine(@"Notification received: {0}", e.Message);
        }

        private static void EventReceiverEventReceived(object sender,
            EventReceiverEventArgs e)
        {
            Console.WriteLine(@"Event received: {0}", e.Message);
        }

        private static void EventProcessorOptions_ExceptionReceived(
            object sender,
            ExceptionReceivedEventArgs e)
        {
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(e.Exception.Message);
            Console.ForegroundColor = ConsoleColor.Green;
        }
    }
}