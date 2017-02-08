using System;
using System.Configuration;

namespace Daishi.Pluralsight.EventHub.WebTrafficGenerator
{
    internal class Program
    {
        // todo: Simple pub/sub style diagram depicting a simple event flowing from system A to B.
        private static void Main(string[] args)
        {
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine(@"Press Ctrl-C to stop the sender process");
            Console.WriteLine(@"Press Enter to start now...");
            Console.ReadLine();

            try
            {
                Console.WriteLine(@"Establishing connection to Event Hub...");

                var eventHubConnectionString =
                    ConfigurationManager.AppSettings["EventHubConnectionString"];
                var numSimulatedHttpRequests =
                    int.Parse(ConfigurationManager.AppSettings["NumSimulatedHttpRequests"]);

                var webTrafficGenerator = new WebTrafficGenerator();
                webTrafficGenerator.SimulatedHttpRequestPublished
                    += WebTrafficGenerator_SimulatedHttpRequestPublished;

                webTrafficGenerator.Generate(
                    eventHubConnectionString,
                    numSimulatedHttpRequests);

                Console.Clear();
                Console.WriteLine(@"Press Ctrl-C to stop the sender process");
                Console.WriteLine();
                Console.WriteLine(
                    $"Generated and published {numSimulatedHttpRequests} " +
                    @"simulated HTTP requests to Event Hub.");

                Console.WriteLine(@"Press Enter to quit...");
                Console.ReadLine();
            }
            catch (Exception exception)
            {
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(exception.Message);
                if (exception.InnerException != null)
                {
                    Console.WriteLine(exception.InnerException.Message);
                }
                Console.ReadLine();
            }
        }

        private static void WebTrafficGenerator_SimulatedHttpRequestPublished(
            object sender,
            WebTrafficGeneratorEventArgs e)
        {
            Console.Clear();
            Console.WriteLine(@"Press Ctrl-C to stop the sender process");
            Console.WriteLine();
            Console.WriteLine($"Published {e.IpAddress} to Event Hub.");
            Console.WriteLine($"Published {e.NumSimulatedHttpRequests} to Event Hub.");
        }
    }
}