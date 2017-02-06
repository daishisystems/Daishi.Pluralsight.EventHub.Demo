using System;

namespace Daishi.Pluralsight.EventHub
{
    /// <summary>
    ///     <see cref="Device" /> refers to a physical device, such as a phone, or
    ///     tablet, from which a HTTP request is issued to our simulated web application.
    /// </summary>
    [Flags]
    public enum Device
    {
        /// <summary>
        ///     <see cref="Unknown" /> represents a device whose type cannot be determined.
        /// </summary>
        Unknown = 0,

        /// <summary>
        ///     <see cref="PersonalComputer" /> represents any given PC.
        /// </summary>
        PersonalComputer = 1,

        /// <summary>
        ///     <see cref="Mac" /> represents any given Apple Mac.
        /// </summary>
        Mac = 2,

        /// <summary>
        ///     <see cref="Phone" /> represents a mobile phone.
        /// </summary>
        Phone = 4,

        /// <summary>
        ///     <see cref="Tablet" /> represents any given tablet device.
        /// </summary>
        Tablet = 8
    }
}