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
// along with this program.  If not, see <https://www.gnu.org/licenses/>.

using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

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
	NoPayload = 0x7F, // No payload; Used for signaling or notifications

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
}

public enum RingBufferFlags : uint
{
	None = 0,
	Shutdown = 1 << 0, // Indicates that the primary endpoint has shut down and no more messages will be processed
}

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public unsafe struct RingBufferHeader // 64 bytes
{
	public ulong SharedMemorySize;     // Total size of the shared memory segment
	public uint BlockCount;            // The number of blocks in the ring buffer
	private fixed byte _reserved1[4];  // Padding to make next field 8-byte aligned
	public ulong BlockSize;            // The size of each block in bytes
	public ulong ProducerHead;         // Offset to the head of the producer
	public ulong ConsumerHead;         // Offset to the head of the consumer
	public RingBufferFlags Flags;      // Bit flags for various states (e.g., shutdown)
	private fixed byte _reserved2[20]; // Padding to make the struct 64 bytes
}

// Each control variable consists of 2 parts:
// (1a) Index:   The block (0 to N-1) the pointer is currently pointing to.
// (1b) Offset:  The byte offset within a block.
// (2)  Version: Distinguishes between cycles through the ring buffer.
[StructLayout(LayoutKind.Explicit, Size = 256, Pack = 64)]
public unsafe struct BlockHeader // 32 bytes
{
	[FieldOffset(0)]
	public ulong Allocated;

	[FieldOffset(64)]
	public ulong Committed;

	[FieldOffset(128)]
	public ulong Reserved;

	[FieldOffset(192)]
	public ulong Consumed;
}

public interface IMessageHeader
{
	PayloadType Type { get; set; }
	ulong Length { get; set; }
}

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public unsafe struct MessageHeader(uint id = 0, PayloadType type = PayloadType.Invalid, ulong length = 0) : IMessageHeader // 16 bytes
{
	public uint Id = id;             // User-defined message identifier
	private PayloadType _type = type;  // Type of the message payload
	private fixed byte _reserved[3]; // Padding to make the struct 16 bytes
	private ulong _length = length;    // Length of the message payload

	public PayloadType Type
	{
		readonly get => this._type;
		set => this._type = value;
	}

	public ulong Length
	{
		readonly get => this._length;
		set => this._length = value;
	}
}

// Wrapper for the default message header
public unsafe class RingBuffer(byte* shmPtr, uint blockCount, ulong blockSize, bool isShmemOwner)
	: RingBuffer<MessageHeader>(shmPtr, blockCount, blockSize, isShmemOwner)
{
}

/// <summary>
/// TODO
/// </summary>
/// <remarks>
/// <para>The shared memory layout is:<br/><br/>
/// <code>
/// | RingBufferHeader | BlockHeader[0] | ... | BlockHeader[N-1] | Block[0] | ... | Block[N-1] |<br/>
/// |                  |                                         | variable |     |  variable  |<br/>
/// |     64 bytes     |            32 bytes each * N            |------- variable * N  -------|<br/>
/// |--------------------------- Headers ------------------------|-------- Message data -------|<br/>
/// |------------------------------- SharedMemorySize(total) ----------------------------------|
/// </code>
/// </para>
/// </remarks>
public unsafe class RingBuffer<TMessageHeader> : IDisposable
	where TMessageHeader : unmanaged, IMessageHeader
{
	// TODO: Message: Header (fixed size) + Payload (variable byte array)
	// TODO: Block struct

	// TODO (Design):
	// - | RingBufferHeader | BlockHeaders[N] | Blocks[N] |
	//   - The ring buffer header stores the head pointers for the producer(s) and consumer(s) in the form of offsets.
	//   - Each block header is a fixed-size struct that stores the four cursors (allocated, committed, reserved, consumed) as offsets within the block, starting from 0.
	//   - Each block is a fixed-size struct that stores a series of [MessageHeader] + [MessagePayload] pairs.
	// - The ring buffer has a global, fixed-size header that stores metadata and flags that are shared between processes.
	//   - One such flag should be to indicate when a server has shut down.
	// - Each block defines a fixed-size memory segment within the ring buffer.
	// - If a message exceeds the remaining space in the current block, move the block-level cursors to the end of the block and advance to the next block.
	// - If the next block (i.e. an empty block) is not large enough to hold the message, throw an exception.
	// - Don't bother cleaning up consume messages. As long as we update the rb-level and block-level pointers correctly, the space will be overwritten when needed.
	// - Advanced users can provide a custom message header. Create a wrapper that uses the default message header so that it's easy to use out of the box.
	// - The ring buffer class exists in an context where shared memory is already created and mapped to the process's address space. So the pointer should be valid at all times.

	private const int MinVersionBits = 16;
	private const int MaxBlockSizeBits = 64 - MinVersionBits; // ~256 TB max block size
	private const ulong MaxBlockSize = (1UL << MaxBlockSizeBits) - 1;

	private static readonly ulong RingBufferHeaderSize = (ulong)sizeof(RingBufferHeader);
	private static readonly ulong BlockHeaderSize = (ulong)sizeof(BlockHeader);
	private static readonly ulong MessageHeaderSize = (ulong)sizeof(TMessageHeader);

	private byte* shmPtr;
	private readonly uint blockCount;
	private readonly ulong blockSize;
	private RingBufferHeader* rbHeaderPtr;
	private readonly byte* blkHeaderStartPtr;
	private readonly byte* blkPayloadStartPtr;
	private readonly bool isShmemOwner;
	private readonly int idxBits;
	private readonly ulong idxMask;
	private readonly int offBits;
	private readonly ulong offMask;
	private readonly int vsnBits;
	private readonly ulong vsnMask;
	private bool disposedValue;

	public RingBuffer(byte* shmPtr, uint blockCount, ulong blockSize, bool isShmemOwner)
	{
		ArgumentNullException.ThrowIfNull(shmPtr);

		// NOTE: Since block count is a uint, it will never be take up space
		// reserved for the version so only check block size here.
		if (blockSize > MaxBlockSize)
			throw new ArgumentOutOfRangeException(nameof(blockSize),
				$"Block size must be less than or equal to {MaxBlockSize} bytes.");

		this.shmPtr = shmPtr;
		this.blockCount = blockCount;
		this.blockSize = blockSize;
		this.isShmemOwner = isShmemOwner;
		this.idxBits = (blockCount > 1) ? (int)Math.Ceiling(Math.Log2(blockCount)) : 1;
		this.idxMask = (this.idxBits < 64) ? ((1UL << this.idxBits) - 1) : ~0UL;
		this.offBits = (blockSize > 1) ? (int)Math.Ceiling(Math.Log2(blockSize)) + 1 : 1;
		this.offMask = (this.offBits < 64) ? ((1UL << this.offBits) - 1) : ~0UL;
		this.vsnBits = Math.Max(MinVersionBits, 64 - Math.Max(this.idxBits, this.offBits));
		this.vsnMask = (this.vsnBits < 64) ? ((1UL << this.vsnBits) - 1) : ~0UL;

		this.rbHeaderPtr = (RingBufferHeader*)shmPtr;
		this.blkHeaderStartPtr = shmPtr + RingBufferHeaderSize;
		this.blkPayloadStartPtr = this.blkHeaderStartPtr + BlockHeaderSize * blockCount;

		// NOTE: Endpoint ensures exclusive creation of the shared
		// memory segment, so no synchronization is needed here
		if (isShmemOwner)
		{
			this.SetBlockHeadersToDefault();
			this.SetMessageRegionToDefault();
		}
	}

	~RingBuffer()
	{
		this.Dispose(false);
	}

	// TODO: Implement a shutting down expression that checks if the shutdown flag is set and prevents further writes.

	public void Dispose()
	{
		this.Dispose(true);
		GC.SuppressFinalize(this);
	}

	// It is the responsibility of Endpoint to prep up the message header and payload.
	// The write/read opearations within the ring buffer deal with atomically dealing
	// with the ring buffer memory and updating the control variables.
	public OpStatus Write(TMessageHeader msgHeader, byte[]? payload = null)
	{
		while (true)
		{
			ulong pHead = Interlocked.Read(ref this.rbHeaderPtr->ProducerHead);
			BlockHeader* blockHdrPtr = (BlockHeader*)(this.blkHeaderStartPtr
				+ BlockHeaderSize * this.GetBlockIdx(pHead));
			byte* blockPtr = this.blkPayloadStartPtr
				+ this.GetBlockIdx(pHead) * this.rbHeaderPtr->BlockSize;
			var entry = new EntryDesc(blockHdrPtr, blockPtr, msgHeader, payload);
			State state = this.AllocateEntry(ref entry);
			switch (state)
			{
				case State.Allocated:
					this.CommitEntry(ref entry);
					return OpStatus.Ok;
				case State.BlockDone:
					switch (this.AdvancePHead(pHead))
					{
						case State.NoEntry:
							return OpStatus.Full;
						case State.NotAvailable:
							return OpStatus.Busy;
						case State.Success:
							continue;
						default:
							return OpStatus.Error;
					}
				default:
					return OpStatus.Error;
			}
		}
	}

	public OpStatus Read(out TMessageHeader msgHeader, out byte[] payload)
	{
		while (true)
		{
			ulong cHead = Interlocked.Read(ref this.rbHeaderPtr->ConsumerHead);
			BlockHeader* blockHdrPtr = (BlockHeader*)(this.blkHeaderStartPtr
				+ BlockHeaderSize * this.GetBlockIdx(cHead));
			byte* blockPtr = this.blkPayloadStartPtr
				+ this.GetBlockIdx(cHead) * this.rbHeaderPtr->BlockSize;
			var entry = new EntryDesc(blockHdrPtr, blockPtr);
			State state = this.ReserveEntry(ref entry);
			switch (state)
			{
				case State.Reserved:
					Tuple<TMessageHeader, byte[]>? result = this.ConsumeEntry(ref entry);
					if (result == null)
						continue;

					msgHeader = result.Item1;
					payload = result.Item2;
					return OpStatus.Ok;
				case State.NoEntry:
					msgHeader = default;
					payload = [];
					return OpStatus.Empty;
				case State.NotAvailable:
					msgHeader = default;
					payload = [];
					return OpStatus.Busy;
				case State.BlockDone:
					if (this.AdvanceCHead(cHead))
						continue;
					else
					{
						msgHeader = default;
						payload = [];
						return OpStatus.Empty;
					}
				default:
					msgHeader = default;
					payload = [];
					return OpStatus.Error;
			}
		}
	}

	protected virtual void Dispose(bool disposing)
	{
		if (this.disposedValue)
			return;

		if (disposing)
		{
			/* Dispose of managed resources here */
			// TODO: Verify we're diposing of everything correctly
			if (this.isShmemOwner)
			{
				Interlocked.Or(ref Unsafe.As<RingBufferFlags, uint>(ref this.rbHeaderPtr->Flags),
					(uint)RingBufferFlags.Shutdown);
			}
		}

		/* Dipose of unmanaged resources here */
		// TODO: Verify we're diposing of everything correctly
		this.shmPtr = null;
		this.rbHeaderPtr = null;
		this.disposedValue = true;
	}

	private struct EntryDesc(BlockHeader* blkHeader, byte* blkPayload, TMessageHeader? msgHeader = null, byte[]? msgPayload = null)
	{
		public TMessageHeader? MsgHeader = msgHeader;
		public byte[]? MsgPayload = msgPayload;
		public BlockHeader* BlockHeader = blkHeader;
		public byte* BlockPayload = blkPayload;
		public ulong PayloadLength;
		public ulong Offset = 0;
	}

	private enum State : byte
	{
		Success = 0,
		BlockDone = 1,
		Allocated = 2,
		Reserved = 3,
		NoEntry = 4,
		NotAvailable = 5,
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private ulong GetBlockIdx(ulong hVal) => hVal & this.idxMask;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private ulong GetBlockVsn(ulong hVal) => (hVal >> this.idxBits) & this.vsnMask;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private ulong PkgHead(ulong idx, ulong vsn) => (idx & this.idxMask) | ((vsn << this.idxBits) & (this.vsnMask << this.idxBits));

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private ulong GetCursorOff(ulong cVal) => cVal & this.offMask;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private ulong GetCursorVsn(ulong cVal) => (cVal >> this.offBits) & this.vsnMask;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private ulong PkgCursor(ulong off, ulong vsn) => (off & this.offMask) | ((vsn << this.offBits) & (this.vsnMask << this.offBits));

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static ulong AtomicMax(ref ulong var, ulong newVal)
	{
		ulong oldVal = Interlocked.Read(ref var);
		if (oldVal >= newVal)
			return oldVal;

		while (oldVal < newVal)
		{
			ulong prev = Interlocked.CompareExchange(ref var, newVal, oldVal);
			if (prev == oldVal)
				break;

			oldVal = prev;
			if (oldVal >= newVal)
				break;
		}

		return oldVal;
	}

	private void SetBlockHeadersToDefault()
	{
		byte* ptr = this.blkHeaderStartPtr;
		byte* endPtr = ptr + BlockHeaderSize * this.rbHeaderPtr->BlockCount;

		// Set all control variables to zero for the first block
		*(BlockHeader*)ptr = new BlockHeader
		{
			Allocated = 0,
			Committed = 0,
			Reserved = 0,
			Consumed = 0,
		};

		ptr += BlockHeaderSize;

		// Set all control variables to block size for the remaining blocks
		while (ptr < endPtr)
		{
			*(BlockHeader*)ptr = new BlockHeader
			{
				Allocated = this.PkgCursor(this.rbHeaderPtr->BlockSize, 0),
				Committed = this.PkgCursor(this.rbHeaderPtr->BlockSize, 0),
				Reserved = this.PkgCursor(this.rbHeaderPtr->BlockSize, 0),
				Consumed = this.PkgCursor(this.rbHeaderPtr->BlockSize, 0),
			};

			ptr += BlockHeaderSize;
		}
	}

	private void SetMessageRegionToDefault()
	{
		// Set the message region to zero
		Unsafe.InitBlockUnaligned(this.blkPayloadStartPtr, 0, (uint)this.rbHeaderPtr->BlockSize * this.rbHeaderPtr->BlockCount);
	}

	private State AllocateEntry(ref EntryDesc entry)
	{
		ulong entrySize = MessageHeaderSize + (entry.MsgHeader?.Length ?? 0);
		ulong allocated = Interlocked.Read(ref entry.BlockHeader->Allocated);
		ulong offset = this.GetCursorOff(allocated);

		if (offset + MessageHeaderSize > this.blockSize)
		{
			// Not enough space for a dummy message header, only move the producer cursors.
			Interlocked.Exchange(ref entry.BlockHeader->Allocated, this.PkgCursor(this.blockSize, this.GetCursorVsn(allocated)));
			Interlocked.Exchange(ref entry.BlockHeader->Committed, this.PkgCursor(this.blockSize, this.GetCursorVsn(allocated)));
			return State.BlockDone;
		}

		if (offset + entrySize > this.blockSize)
		{
			// Not enough space for the entry. Move the producer cursors and create a dummy entry
			// to inform the consumers to proceed to the next block.
			Interlocked.Exchange(ref entry.BlockHeader->Allocated, this.PkgCursor(this.blockSize, this.GetCursorVsn(allocated)));

			// Create a dummy entry
			*(TMessageHeader*)(entry.BlockPayload + offset) = new TMessageHeader
			{
				Type = PayloadType.Invalid,
				Length = this.blockSize - offset - MessageHeaderSize,
			};

			Interlocked.Exchange(ref entry.BlockHeader->Committed, this.PkgCursor(this.blockSize, this.GetCursorVsn(allocated)));
			return State.BlockDone;
		}

		// Enough space for the full entry
		ulong oldCursor = Interlocked.Add(ref entry.BlockHeader->Allocated, entrySize) - entrySize;
		if (this.GetCursorOff(oldCursor) >= this.blockSize)
			return State.BlockDone;

		entry.Offset = this.GetCursorOff(oldCursor);
		return State.Allocated;
	}

	private void CommitEntry(ref EntryDesc entry)
	{
		// Write the message header
		*(TMessageHeader*)(entry.BlockPayload + entry.Offset) = (TMessageHeader)entry.MsgHeader!;

		// Write the message payload if it exists
		if (entry.MsgPayload != null && entry.MsgHeader != null && entry.MsgHeader?.Length > 0)
		{
			fixed (byte* src = entry.MsgPayload)
			{
				Buffer.MemoryCopy(src, entry.BlockPayload + entry.Offset + MessageHeaderSize, entry.MsgHeader.Value.Length, entry.MsgHeader.Value.Length);
			}
		}

		// Update the committed cursor
		ulong entrySize = MessageHeaderSize + (entry.MsgHeader?.Length ?? 0);
		Interlocked.Add(ref entry.BlockHeader->Committed, entrySize);
	}

	private State AdvancePHead(ulong pHead)
	{
		ulong nextIdx = (this.GetBlockIdx(pHead) + 1) % this.blockCount;
		ulong nextVsn = this.GetBlockVsn(pHead) + ((nextIdx == 0) ? 1UL : 0UL);
		BlockHeader* nextBlkHdrPtr = (BlockHeader*)(this.blkHeaderStartPtr + BlockHeaderSize * nextIdx);

		// Check if the next block is available (i.e., fully consumed)
		ulong consumed = Interlocked.Read(ref nextBlkHdrPtr->Consumed);
		if (this.GetCursorVsn(consumed) < this.GetBlockVsn(pHead) ||
			(this.GetCursorVsn(consumed) == this.GetBlockVsn(pHead) && this.GetCursorOff(consumed) != this.blockSize))
		{
			ulong reserved = Interlocked.Read(ref nextBlkHdrPtr->Reserved);
			return this.GetCursorOff(reserved) == this.GetCursorOff(consumed) ? State.NoEntry : State.NotAvailable;
		}

		AtomicMax(ref nextBlkHdrPtr->Committed, this.PkgCursor(0, this.GetBlockVsn(pHead) + 1));
		AtomicMax(ref nextBlkHdrPtr->Allocated, this.PkgCursor(0, this.GetBlockVsn(pHead) + 1));
		AtomicMax(ref this.rbHeaderPtr->ProducerHead, this.PkgHead(nextIdx, nextVsn));

		return State.Success;
	}

	private State ReserveEntry(ref EntryDesc entry)
	{
		while (true)
		{
			ulong reserved = Interlocked.Read(ref entry.BlockHeader->Reserved);
			if (this.GetCursorOff(reserved) + MessageHeaderSize <= this.blockSize)
			{
				ulong committed = Interlocked.Read(ref entry.BlockHeader->Committed);
				if (this.GetCursorOff(reserved) == this.GetCursorOff(committed))
					return State.NoEntry;

				if (this.GetCursorOff(committed) < this.blockSize)
				{
					ulong allocated = Interlocked.Read(ref entry.BlockHeader->Allocated);
					if (this.GetCursorOff(allocated) != this.GetCursorOff(committed))
						return State.NotAvailable;
				}

				// Try to reserve the next message entry in the block
				ulong offset = this.GetCursorOff(reserved);
				TMessageHeader msgHeader = *(TMessageHeader*)(entry.BlockPayload + offset);
				ulong entrySize = MessageHeaderSize + msgHeader.Length;

				if (Interlocked.CompareExchange(ref entry.BlockHeader->Reserved, this.PkgCursor(offset + entrySize, this.GetCursorVsn(reserved)), reserved) == reserved)
				{
					// Successfully reserved space for the message header
					entry.Offset = offset;
					entry.PayloadLength = entry.Offset + entrySize;

					if (msgHeader.Type == PayloadType.Invalid)
					{
						Interlocked.Exchange(ref entry.BlockHeader->Consumed, this.PkgCursor(offset + entrySize, this.GetCursorVsn(reserved)));
						return State.BlockDone;
					}

					return State.Reserved;
				}
				else
					continue; // Retry if the reservation failed
			}

			return State.BlockDone;
		}
	}

	private Tuple<TMessageHeader, byte[]> ConsumeEntry(ref EntryDesc entry)
	{
		ulong entrySize = MessageHeaderSize + entry.PayloadLength;
		Interlocked.Add(ref entry.BlockHeader->Consumed, entrySize);

		TMessageHeader msgHeader = *(TMessageHeader*)(entry.BlockPayload + entry.Offset);
		byte[] msgPayload = [];

		if (msgHeader.Length > 0)
		{
			msgPayload = new byte[(int)msgHeader.Length];
			fixed (byte* dst = msgPayload)
			{
				Buffer.MemoryCopy(
					entry.BlockPayload + entry.Offset + MessageHeaderSize,
					dst,
					msgHeader.Length,
					msgHeader.Length
				);
			}
		}

		return Tuple.Create(msgHeader, msgPayload);
	}

	private bool AdvanceCHead(ulong cHead)
	{
		ulong nextIdx = (this.GetBlockIdx(cHead) + 1) % this.blockCount;
		ulong nextVsn = this.GetBlockVsn(cHead) + ((nextIdx == 0) ? 1UL : 0UL);
		BlockHeader* nextBlkHdrPtr = (BlockHeader*)(this.blkHeaderStartPtr + BlockHeaderSize * nextIdx);

		ulong committed = Interlocked.Read(ref nextBlkHdrPtr->Committed);

		// Only advance the consumer head if the next block is fully committed
		if (this.GetCursorVsn(committed) != this.GetBlockVsn(cHead) + 1)
			return false;

		AtomicMax(ref nextBlkHdrPtr->Consumed, this.PkgCursor(0, this.GetBlockVsn(cHead) + 1));
		AtomicMax(ref nextBlkHdrPtr->Reserved, this.PkgCursor(0, this.GetBlockVsn(cHead) + 1));
		AtomicMax(ref this.rbHeaderPtr->ConsumerHead, this.PkgHead(nextIdx, nextVsn));
		return true;
	}
}
