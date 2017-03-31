#region Includes

using System;

#endregion

namespace Daishi.Pluralsight.EventHub {
    /// <summary>
    ///     <see cref="DeviceType" /> refers to a physical device, such as a phone, tablet, etc., 
    ///     from which a HTTP request is issued to our application.
    /// </summary>
    [Flags]
    public enum DeviceType {
        /// <summary>
        ///     <see cref="Unknown" /> represents a device whose type cannot be determined.
        /// </summary>
        Unknown = 0,

        /// <summary>
        ///     <see cref="PersonalComputer" /> represents any given PC.
        /// </summary>
        PersonalComputer = 1,

        /// <summary>
        ///     <see cref="Laptop" /> represents any given laptop device.
        /// </summary>
        Laptop = 2,

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