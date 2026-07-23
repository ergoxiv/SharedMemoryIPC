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
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace SharedMemoryIPC;

// Each control variable consists of 2 parts:
// (1a) Index:   The block (0 to N-1) the pointer is currently pointing to.
// (1b) Offset:  The byte offset within a block.
// (2)  Version: Distinguishes between cycles through the ring buffer.
[StructLayout(LayoutKind.Explicit, Size = 256, Pack = 64)]
public unsafe struct MpmcBlockHeader // 256 bytes
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

// Wrapper for the default message header
public unsafe class MpmcRingBuffer(byte* shmPtr, uint blockCount, ulong blockSize, bool isShmemOwner)
	: MpmcRingBuffer<MessageHeader>(shmPtr, blockCount, blockSize, isShmemOwner)
{
}

/// <summary>
/// The core of the shared-memory IPC mechanism: a multi-producer, multi-consumer (MPMC) ring buffer.
/// </summary>
/// <remarks>
/// <para>The shared memory layout is:<br/><br/>
/// <code>
/// | RingBufferHeader | MpmcBlockHeader[0] | ... | MpmcBlockHeader[N-1] | Block[0] | ... | Block[N-1] |<br/>
/// |                  |                                         | variable |     |  variable  |<br/>
/// |     64 bytes     |            32 bytes each * N            |------- variable * N  -------|<br/>
/// |--------------------------- Headers ------------------------|-------- Message data -------|<br/>
/// |------------------------------- SharedMemorySize(total) ----------------------------------|
/// </code>
/// </para>
/// </remarks>
public unsafe class MpmcRingBuffer<TMessageHeader> : IRingBuffer<TMessageHeader>
	where TMessageHeader : unmanaged, IMessageHeader
{
	// Design notes:
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
	private static readonly ulong BlockHeaderSize = (ulong)sizeof(MpmcBlockHeader);
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
	private readonly ulong vsnShiftedIdxMask;
	private readonly ulong vsnShiftedOffMask;
	private bool disposedValue;

	/// <summary>
	/// Initializes a new instance of the <see cref="MpmcRingBuffer{TMessageHeader}"/> class.
	/// </summary>
	/// <param name="shmPtr">The pointer to the start of the shared memory segment.</param>
	/// <param name="blockCount">The number of blocks in the ring buffer.</param>
	/// <param name="blockSize">The size of each block in bytes.</param>
	/// <param name="isShmemOwner">
	/// A value indicating whether this instance is the owner of the shared memory segment.
	/// This affects whether the ring buffer initializes the memory region.
	/// </param>
	/// <exception cref="ArgumentOutOfRangeException">
	/// Thrown if <paramref name="blockSize"/> exceeds the maximum allowed size.
	/// </exception>
	public MpmcRingBuffer(byte* shmPtr, uint blockCount, ulong blockSize, bool isShmemOwner)
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
		this.vsnShiftedIdxMask = this.vsnMask << this.idxBits;
		this.vsnShiftedOffMask = this.vsnMask << this.offBits;

		this.rbHeaderPtr = (RingBufferHeader*)shmPtr;
		this.blkHeaderStartPtr = shmPtr + RingBufferHeaderSize;
		this.blkPayloadStartPtr = this.blkHeaderStartPtr + BlockHeaderSize * blockCount;

		// NOTE: The endpoint ensures exclusive creation of the shared
		// memory segment, so no synchronization is needed here
		if (isShmemOwner)
		{
			this.SetBlockHeadersToDefault();
			this.SetMessageRegionToDefault();
		}
	}

	~MpmcRingBuffer()
	{
		this.Dispose(false);
	}

	/// <summary>
	/// Disposes the ring buffer and releases associated resources.
	/// </summary>
	public void Dispose()
	{
		this.Dispose(true);
		GC.SuppressFinalize(this);
	}

	/// <inheritdoc/>
	public OpStatus Write(TMessageHeader msgHeader, ReadOnlySpan<byte> payload = default)
	{
		if ((this.rbHeaderPtr->Flags & RingBufferFlags.Shutdown) != 0)
		{
			msgHeader = default;
			payload = [];
			return OpStatus.Error;
		}

		for (; ; )
		{
			ulong pHead = Volatile.Read(ref this.rbHeaderPtr->ProducerHead);
			MpmcBlockHeader* blockHdrPtr = (MpmcBlockHeader*)(this.blkHeaderStartPtr
				+ BlockHeaderSize * this.GetBlockIdx(pHead));
			byte* blockPtr = this.blkPayloadStartPtr
				+ this.GetBlockIdx(pHead) * this.blockSize;
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

	/// <inheritdoc/>
	public OpStatus Read(out TMessageHeader msgHeader, out ReadOnlySpan<byte> payload)
	{
		if ((this.rbHeaderPtr->Flags & RingBufferFlags.Shutdown) != 0)
		{
			msgHeader = default;
			payload = [];
			return OpStatus.Error;
		}

		for (; ; )
		{
			ulong cHead = Volatile.Read(ref this.rbHeaderPtr->ConsumerHead);
			MpmcBlockHeader* blockHdrPtr = (MpmcBlockHeader*)(this.blkHeaderStartPtr
				+ BlockHeaderSize * this.GetBlockIdx(cHead));
			byte* blockPtr = this.blkPayloadStartPtr
				+ this.GetBlockIdx(cHead) * this.blockSize;
			var entry = new EntryDesc(blockHdrPtr, blockPtr);
			State state = this.ReserveEntry(ref entry);
			switch (state)
			{
				case State.Reserved:
					this.ConsumeEntry(ref entry);
					msgHeader = entry.MsgHeader;
					payload = entry.MsgPayload;
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
			if (this.isShmemOwner)
			{
				Interlocked.Or(ref Unsafe.As<RingBufferFlags, uint>(ref this.rbHeaderPtr->Flags),
					(uint)RingBufferFlags.Shutdown);
			}
		}

		/* Dipose of unmanaged resources here */
		this.shmPtr = null;
		this.rbHeaderPtr = null;
		this.disposedValue = true;
	}

	private ref struct EntryDesc(MpmcBlockHeader* blkHeader, byte* blkPayload, TMessageHeader msgHeader = default, ReadOnlySpan<byte> msgPayload = default)
	{
		public TMessageHeader MsgHeader = msgHeader;
		public ReadOnlySpan<byte> MsgPayload = msgPayload;
		public MpmcBlockHeader* BlockHeader = blkHeader;
		public byte* BlockPayload = blkPayload;
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
	private ulong GetCursorOff(ulong cVal) => cVal & this.offMask;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private ulong GetCursorVsn(ulong cVal) => (cVal >> this.offBits) & this.vsnMask;

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private ulong PkgHead(ulong idx, ulong vsn) => (idx & this.idxMask) | ((vsn << this.idxBits) & this.vsnShiftedIdxMask);

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private ulong PkgCursor(ulong off, ulong vsn) => (off & this.offMask) | ((vsn << this.offBits) & this.vsnShiftedOffMask);

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static ulong AtomicMax(ref ulong location, ulong newVal)
	{
		ulong oldVal = Volatile.Read(ref location);

		if (oldVal >= newVal)
			return oldVal;

		while (oldVal < newVal)
		{
			ulong prev = Interlocked.CompareExchange(ref location, newVal, oldVal);
			if (prev == oldVal)
				return newVal;

			oldVal = prev;
		}

		return oldVal;
	}

	private void SetBlockHeadersToDefault()
	{
		byte* ptr = this.blkHeaderStartPtr;
		byte* endPtr = ptr + BlockHeaderSize * this.rbHeaderPtr->BlockCount;

		// Set all control variables to zero for the first block
		*(MpmcBlockHeader*)ptr = new MpmcBlockHeader
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
			*(MpmcBlockHeader*)ptr = new MpmcBlockHeader
			{
				Allocated = this.PkgCursor(this.blockSize, 0),
				Committed = this.PkgCursor(this.blockSize, 0),
				Reserved = this.PkgCursor(this.blockSize, 0),
				Consumed = this.PkgCursor(this.blockSize, 0),
			};

			ptr += BlockHeaderSize;
		}
	}

	private void SetMessageRegionToDefault()
	{
		// Set the message region to zero
		Unsafe.InitBlockUnaligned(this.blkPayloadStartPtr, 0, (uint)this.blockSize * this.rbHeaderPtr->BlockCount);
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private State AllocateEntry(ref EntryDesc entry)
	{
		ulong allocated = Volatile.Read(ref entry.BlockHeader->Allocated);
		ulong entrySize = MessageHeaderSize + entry.MsgHeader.Length;
		ulong allocatedOff = this.GetCursorOff(allocated);
		ulong endCursor = this.PkgCursor(this.blockSize, this.GetCursorVsn(allocated));

		if (allocatedOff + MessageHeaderSize > this.blockSize)
		{
			// Not enough space for a dummy message header, only move the producer cursors.
			Interlocked.Exchange(ref entry.BlockHeader->Allocated, endCursor);
			Interlocked.Exchange(ref entry.BlockHeader->Committed, endCursor);
			return State.BlockDone;
		}

		if (allocatedOff + entrySize > this.blockSize)
		{
			// Not enough space for the entry. Move the producer cursors and create a dummy entry
			// to inform the consumers to proceed to the next block.
			Interlocked.Exchange(ref entry.BlockHeader->Allocated, endCursor);

			// Create a dummy entry
			*(TMessageHeader*)(entry.BlockPayload + allocatedOff) = new TMessageHeader
			{
				Type = PayloadType.Invalid,
				Length = this.blockSize - allocatedOff - MessageHeaderSize,
			};

			Interlocked.Exchange(ref entry.BlockHeader->Committed, endCursor);
			return State.BlockDone;
		}

		// Enough space for the full entry
		ulong oldCursor = Interlocked.Add(ref entry.BlockHeader->Allocated, entrySize) - entrySize;
		if (this.GetCursorOff(oldCursor) >= this.blockSize)
			return State.BlockDone;

		entry.Offset = this.GetCursorOff(oldCursor);
		return State.Allocated;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void CommitEntry(ref EntryDesc entry)
	{
		// Write the message header
		*(TMessageHeader*)(entry.BlockPayload + entry.Offset) = entry.MsgHeader;

		// Write the message payload if it exists
		if (entry.MsgHeader.Length > 0 && !entry.MsgPayload.IsEmpty)
		{
			ref byte src = ref MemoryMarshal.GetReference(entry.MsgPayload);
			ref byte dst = ref *(entry.BlockPayload + entry.Offset + MessageHeaderSize);
			Unsafe.CopyBlockUnaligned(ref dst, ref src, (uint)entry.MsgHeader.Length);
		}

		// Update the committed cursor
		ulong entrySize = MessageHeaderSize + entry.MsgHeader.Length;
		Interlocked.Add(ref entry.BlockHeader->Committed, entrySize);
	}

	private State AdvancePHead(ulong pHead)
	{
		ulong nextIdx = (this.GetBlockIdx(pHead) + 1) % this.blockCount;
		ulong pHeadVsn = this.GetBlockVsn(pHead);
		ulong nextVsn = pHeadVsn + ((nextIdx == 0) ? 1UL : 0UL);
		MpmcBlockHeader* nextBlkHdrPtr = (MpmcBlockHeader*)(this.blkHeaderStartPtr + BlockHeaderSize * nextIdx);

		// Check if the next block is available (i.e., fully consumed)
		ulong consumed = Volatile.Read(ref nextBlkHdrPtr->Consumed);
		if (this.GetCursorVsn(consumed) < pHeadVsn ||
			(this.GetCursorVsn(consumed) == pHeadVsn && this.GetCursorOff(consumed) != this.blockSize))
		{
			ulong reserved = Volatile.Read(ref nextBlkHdrPtr->Reserved);
			return this.GetCursorOff(reserved) == this.GetCursorOff(consumed) ? State.NoEntry : State.NotAvailable;
		}

		AtomicMax(ref nextBlkHdrPtr->Committed, this.PkgCursor(0, pHeadVsn + 1));
		AtomicMax(ref nextBlkHdrPtr->Allocated, this.PkgCursor(0, pHeadVsn + 1));
		AtomicMax(ref this.rbHeaderPtr->ProducerHead, this.PkgHead(nextIdx, nextVsn));

		return State.Success;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private State ReserveEntry(ref EntryDesc entry)
	{
		for (; ; )
		{
			ulong reserved = Volatile.Read(ref entry.BlockHeader->Reserved);
			ulong reservedOff = this.GetCursorOff(reserved);

			if (reservedOff + MessageHeaderSize <= this.blockSize)
			{
				ulong committed = Volatile.Read(ref entry.BlockHeader->Committed);
				if (reservedOff == this.GetCursorOff(committed))
					return State.NoEntry;

				if (this.GetCursorOff(committed) < this.blockSize)
				{
					ulong allocated = Volatile.Read(ref entry.BlockHeader->Allocated);
					if (this.GetCursorOff(allocated) != this.GetCursorOff(committed))
						return State.NotAvailable;
				}

				// Try to reserve the next message entry in the block
				entry.MsgHeader = *(TMessageHeader*)(entry.BlockPayload + reservedOff);
				ulong entrySize = MessageHeaderSize + entry.MsgHeader.Length;

				if (Interlocked.CompareExchange(ref entry.BlockHeader->Reserved, this.PkgCursor(reservedOff + entrySize, this.GetCursorVsn(reserved)), reserved) == reserved)
				{
					// Successfully reserved space for the message header
					entry.Offset = reservedOff;

					if (entry.MsgHeader.Type == PayloadType.Invalid)
					{
						Interlocked.Exchange(ref entry.BlockHeader->Consumed, this.PkgCursor(reservedOff + entrySize, this.GetCursorVsn(reserved)));
						return State.BlockDone;
					}

					return State.Reserved;
				}
				else
					continue; // Retry if the reservation failed
			}

			Interlocked.Exchange(ref entry.BlockHeader->Reserved, this.PkgCursor(this.blockSize, this.GetCursorVsn(reserved)));
			Interlocked.Exchange(ref entry.BlockHeader->Consumed, this.PkgCursor(this.blockSize, this.GetCursorVsn(reserved)));
			return State.BlockDone;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private void ConsumeEntry(ref EntryDesc entry)
	{
		ulong entrySize = MessageHeaderSize + entry.MsgHeader.Length;
		Interlocked.Add(ref entry.BlockHeader->Consumed, entrySize);

		if (entry.MsgHeader.Length > 0)
		{
			entry.MsgPayload = new ReadOnlySpan<byte>(
				entry.BlockPayload + entry.Offset + MessageHeaderSize,
				(int)entry.MsgHeader.Length);
		}
	}

	private bool AdvanceCHead(ulong cHead)
	{
		ulong nextIdx = (this.GetBlockIdx(cHead) + 1) % this.blockCount;
		ulong cHeadVsn = this.GetBlockVsn(cHead);
		ulong nextVsn = cHeadVsn + ((nextIdx == 0) ? 1UL : 0UL);
		MpmcBlockHeader* nextBlkHdrPtr = (MpmcBlockHeader*)(this.blkHeaderStartPtr + BlockHeaderSize * nextIdx);

		ulong committed = Volatile.Read(ref nextBlkHdrPtr->Committed);

		// Only advance the consumer head if the next block is fully committed
		if (this.GetCursorVsn(committed) != cHeadVsn + 1)
			return false;

		AtomicMax(ref nextBlkHdrPtr->Consumed, this.PkgCursor(0, cHeadVsn + 1));
		AtomicMax(ref nextBlkHdrPtr->Reserved, this.PkgCursor(0, cHeadVsn + 1));
		AtomicMax(ref this.rbHeaderPtr->ConsumerHead, this.PkgHead(nextIdx, nextVsn));
		return true;
	}
}
