// SPDX-License-Identifier: GPL-3.0-only
// Copyright (C) 2025  ergoxiv <ergo.ffxiv@gmail.com>
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, Version 3.0 of the License.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY, without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program. If not, see <https://www.gnu.org/licenses/>.

using System.Runtime.InteropServices;

namespace SharedMemoryIPC;

/// <summary>
/// The standard message header for messages in the ring buffer.
/// You can define your own message header to better suit your
/// use case by creating a struct that implements <see cref="IMessageHeader"/>.
/// </summary>
/// <param name="id">
/// The user-defined message identifier.
/// </param>
/// <param name="type">
/// The type of the message payload.
/// </param>
/// <param name="length">
/// The length of the message payload in bytes.
/// </param>
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public unsafe struct MessageHeader(uint id = 0, PayloadType type = PayloadType.Invalid, ulong length = 0) : IMessageHeader // 16 bytes
{
	public uint Id = id;              // User-defined message identifier
	private PayloadType _type = type; // Type of the message payload
	private fixed byte _reserved[3];  // Padding to make the struct 16 bytes
	private ulong _length = length;   // Length of the message payload

	/// <inheritdoc/>
	public PayloadType Type
	{
		readonly get => this._type;
		set => this._type = value;
	}

	/// <inheritdoc/>
	public ulong Length
	{
		readonly get => this._length;
		set => this._length = value;
	}
}
