using System;

namespace Daishi.Pluralsight.EventHub.WebTrafficGenerator
{
    /// <summary>
    ///     <see cref="SimulatedHttpRequestPublishedEventArgs" /> instances are
    ///     marshaled through
    ///     <see cref="WebTrafficGenerator.SimulatedHttpRequestPublished" /> events.
    /// </summary>
    internal class SimulatedHttpRequestPublishedEventArgs : EventArgs
    {
        /// <summary>
        ///     <see cref="NumSimulatedHttpRequests" /> is the current count of
        ///     <see cref="SimulatedHttpRequest" /> instances that have been instantiated.
        /// </summary>
        public int NumSimulatedHttpRequests
        {
            get;
            set;
        }

        /// <summary>
        ///     <see cref="IpAddress" /> is the IP address pertaining to the latest
        ///     instantiated <see cref="SimulatedHttpRequest" />.
        /// </summary>
        public string IpAddress
        {
            get;
            set;
        }
    }
}