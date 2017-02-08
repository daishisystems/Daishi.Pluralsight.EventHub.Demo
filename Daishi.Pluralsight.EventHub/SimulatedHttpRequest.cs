using System;

namespace Daishi.Pluralsight.EventHub
{
    /// <summary>
    ///     <see cref="SimulatedHttpRequest" /> is an event that contains metadata
    ///     originating from a simulated, up-stream web application.
    /// </summary>
    public class SimulatedHttpRequest
    {
        /// <summary>
        ///     <see cref="IpAddress" /> is the IPv4 address associated with an up-stream
        ///     HTTP request.
        /// </summary>
        public string IpAddress
        {
            get;
            set;
        }

        /// <summary>
        ///     <see cref="Time" /> is the local time upon which this instance was created.
        /// </summary>
        /// <remarks>Azure components favour time expressed in ISO-8601 format.</remarks>
        public DateTime Time
        {
            get;
            set;
        }

        /// <summary>
        ///     <see cref="Device" /> represents the physical device from which an
        ///     up-stream HTTP request originated.
        /// </summary>
        public Device Device
        {
            get;
            set;
        }

        /// <summary>Returns a <see cref="string" /> that represents the current instance.</summary>
        /// <returns>A <see cref="string" /> instance that represents the current instance.</returns>
        public override string ToString()
        {
            return $"{Device}: {IpAddress}";
        }

        /// <summary>
        ///     <see cref="GenerateRandom" /> returns a randomly-generated
        ///     <see cref="SimulatedHttpRequest" /> instance.
        /// </summary>
        /// <param name="random">
        ///     <see cref="Random" /> is a <see cref="Random" /> instance
        ///     used to generate <see cref="SimulatedHttpRequest" /> properties.
        /// </param>
        /// <returns>A randomly-generated <see cref="SimulatedHttpRequest" /> instance.</returns>
        public static SimulatedHttpRequest GenerateRandom(Random random)
        {
            return new SimulatedHttpRequest
            {
                IpAddress = GenerateRandomIpAddress(random),
                Device = GenerateRandomDevice(random),
                Time = DateTime.UtcNow
            };
        }

        /// <summary>
        ///     <see cref="GenerateRandomIpAddress" /> returns a randomly-generated IP
        ///     address in <see cref="string" />-format.
        /// </summary>
        /// <param name="random">
        ///     <see cref="random" /> is a <see cref="Random" /> instance
        ///     that produces the required IP address.
        /// </param>
        /// <returns>A randomly-generated IP address in <see cref="string" />-format.</returns>
        private static string GenerateRandomIpAddress(Random random)
        {
            return $"{random.Next(0, 255)}." +
                   $"{random.Next(0, 255)}." +
                   $"{random.Next(0, 255)}." +
                   $"{random.Next(0, 255)}";
        }

        /// <summary>
        ///     <see cref="GenerateRandomDevice" /> returns a randomly-generated
        ///     <see cref="Device" /> instance.
        /// </summary>
        /// <param name="random">
        ///     <see cref="random" /> is a <see cref="Random" /> instance
        ///     that produces the required <see cref="Device" /> instance.
        /// </param>
        /// <returns>A randomly-generated <see cref="Device" /> instance.</returns>
        private static Device GenerateRandomDevice(Random random)
        {
            var values = Enum.GetValues(typeof (Device));
            return (Device) values.GetValue(random.Next(1, values.Length));
        }
    }
}