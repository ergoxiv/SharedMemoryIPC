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

using System;
using System.Runtime.InteropServices;

namespace SharedMemoryIPC;

public enum OpStatus : byte
{
	Ok = 0,
	Busy = 1,
	Empty = 2,
	Full = 3,
	Error = 4,
}

public enum PayloadType : byte
{
	Invalid = 0, // No payload; Used for dummy entries to pad the ring buffer blocks
	Bit = 1,
	Int8 = 2,
	UInt8 = 3,
	Int16 = 4,
	UInt16 = 5,
	Int20 = 6,
	UInt20 = 7,
	Int24 = 8,
	UInt24 = 9,
	Int32 = 10,
	UInt32 = 11,
	Int40 = 12,
	UInt40 = 13,
	Int48 = 14,
	UInt48 = 15,
	Int56 = 16,
	UInt56 = 17,
	Int64 = 18,
	UInt64 = 19,
	Float16 = 20, // Half (IEEE 754, bfloat16)
	Float32 = 21, // Single (IEEE 754)
	Float64 = 22, // Double (IEEE 754)
	Float128 = 23, // Quadriple (IEEE 754)
	Float256 = 24, // Octuple (IEEE 754)
	AsciiString = 25,
	UTF8String = 26,
	UTF16String = 27,
	Guid = 28, // 128-bit globally unique identifier
	Blob = 30, // Arbitrary binary data; Intended for sending unmanaged structs or serialized objects

	// Special types
	Command = 0x71,        // Command; Optional payload (e.g., command invocation)
	Config = 0x72,         // Configure; Optional payload (e.g., config data)
	Event = 0x73,          // Event/notification; Optional payload (e.g., event data)
	Request = 0x74,        // Request; Optional payload (e.g., request parameters)
	Heartbeat = 0x75,      // Heartbeat; No payload
	Retry = 0x76,          // Retry; No payload
	Error = 0x77,          // Error; Optional payload (e.g., error message)
	Syn = 0x78,            // Synchronization; Optional payload (e.g., timestamp)
	Fin = 0x79,            // Finish/close connection; No payload
	Rst = 0x7A,            // Reset connection; No payload
	EventSubscribe = 0x7B, // Event subscription; Optional payload (e.g., event identifier)
	Register = 0x7C,       // Register; Optional payload (e.g., registration info)
	Hello = 0x7D,          // Hello; No payload
	Ack = 0x7E,            // Acknowledgment; No payload
	NoPayload = 0x7F,      // No payload; Used for signaling or notifications

	// NOTE: Limit to 127 maximum allowed types.
	// [!] The last bit is reserved to mark payload as single object (0)/object array (1).
	BitArray = Bit | 0x80,
	Int8Array = Int8 | 0x80,
	UInt8Array = UInt8 | 0x80,
	Int16Array = Int16 | 0x80,
	UInt16Array = UInt16 | 0x80,
	Int20Array = Int20 | 0x80,
	UInt20Array = UInt20 | 0x80,
	Int24Array = Int24 | 0x80,
	UInt24Array = UInt24 | 0x80,
	Int32Array = Int32 | 0x80,
	UInt32Array = UInt32 | 0x80,
	Int40Array = Int40 | 0x80,
	UInt40Array = UInt40 | 0x80,
	Int48Array = Int48 | 0x80,
	UInt48Array = UInt48 | 0x80,
	Int56Array = Int56 | 0x80,
	UInt56Array = UInt56 | 0x80,
	Int64Array = Int64 | 0x80,
	UInt64Array = UInt64 | 0x80,
	Float16Array = Float16 | 0x80,
	Float32Array = Float32 | 0x80,
	Float64Array = Float64 | 0x80,
	Float128Array = Float128 | 0x80,
	Float256Array = Float256 | 0x80,
	AsciiStringArray = AsciiString | 0x80, // Intended for ASCII strings split by null terminators
	UTF8StringArray = UTF8String | 0x80,   // Intended for UTF-8 strings split by null terminators
	UTF16StringArray = UTF16String | 0x80, // Intended for UTF-16 strings split by null terminators
	GuidArray = Guid | 0x80,
	BlobArray = Blob | 0x80,

	EventUnsubscribe = 0xFB, // Event unsubscription; Optional payload (e.g., event identifier)
	Unregister = 0xFC,       // Unregister; Optional payload (e.g., unregistration info)
	Bye = 0xFD,              // Goodbye; No payload
	NAck = 0xFE,             // Negative acknowledgment; No payload
}

public enum RingBufferFlags : uint
{
	None = 0,
	Shutdown = 1 << 0, // Indicates that the primary endpoint has shut down and no more messages will be processed
}

public enum RingBufferMode : uint
{
	Mpmc = 0,
	Spsc = 1,
}

[StructLayout(LayoutKind.Explicit, Size = 264, Pack = 8)]
public unsafe struct RingBufferHeader // 264 bytes
{
	// Cache line 0: Metadata
	[FieldOffset(0)]
	public ulong SharedMemorySize; // Total size of the shared memory segment

	[FieldOffset(8)]
	public uint BlockCount;        // The number of blocks in the ring buffer

	[FieldOffset(16)]
	public ulong BlockSize;        // The size of each block in bytes

	[FieldOffset(24)]
	public RingBufferFlags Flags;  // Bit flags for various states (e.g., shutdown)

	[FieldOffset(28)]
	public RingBufferMode Mode;    // Operating mode (MPMC vs SPSC)

	// Cache line 1: ProducerHead
	[FieldOffset(64)]
	public ulong ProducerHead;     // Offset to the head of the producer

	// Cache line 2: ConsumerHead
	[FieldOffset(128)]
	public ulong ConsumerHead;     // Offset to the head of the consumer

	[FieldOffset(192)]
	public int ReaderWaiting;      // Flag indicating if any reader is waiting (0 = IDLE, 1 = WAITING)

	[FieldOffset(256)]
	public int WriterWaiting;      // Flag indicating if any writer is waiting (0 = IDLE, 1 = WAITING)
}

/// <summary>
/// Defines an interface for ring buffer implementations.
/// </summary>
/// <typeparam name="TMessageHeader">The message header type.</typeparam>
public interface IRingBuffer<TMessageHeader> : IDisposable
	where TMessageHeader : unmanaged, IMessageHeader
{
	/// <summary>
	/// Attempts to enqueue a message into the ring buffer.
	/// </summary>
	/// <param name="msgHeader">
	/// The message header containing metadata about the message.
	/// </param>
	/// <param name="payload">
	/// The payload of the message.
	/// </param>
	/// <returns>
	/// The operation status indicating the result of the write attempt.
	/// </returns>
	/// <remarks>
	/// It is the responsibility of the caller to ensure that the message
	/// header and payload are correctly prepared before calling this method.
	/// </remarks>
	OpStatus Write(TMessageHeader msgHeader, ReadOnlySpan<byte> payload = default);

	/// <summary>
	/// Attempts to dequeue the next available message from the ring buffer.
	/// </summary>
	/// <param name="msgHeader">
	/// Output parameter to store the message header of the dequeued message.
	/// </param>
	/// <param name="payload">
	/// Output parameter to store the payload of the dequeued message.
	/// </param>
	/// <returns>
	/// The operation status indicating the result of the read attempt.
	/// </returns>
	OpStatus Read(out TMessageHeader msgHeader, out ReadOnlySpan<byte> payload);
}
