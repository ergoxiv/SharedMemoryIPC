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
[StructLayout(LayoutKind.Explicit, Size = 128, Pack = 64)]
public unsafe struct SpscBlockHeader // 128 bytes
{
	[FieldOffset(0)]
	public ulong ProducerField;

	[FieldOffset(64)]
	public ulong ConsumerField;
}

// Wrapper for the default message header
public unsafe class SpscRingBuffer(byte* shmPtr, uint blockCount, ulong blockSize, bool isShmemOwner)
	: SpscRingBuffer<MessageHeader>(shmPtr, blockCount, blockSize, isShmemOwner)
{
}

/// <summary>
/// The core of the shared-memory IPC mechanism: a single-producer, single-consumer (SPSC) ring buffer.
/// </summary>
/// <remarks>
/// <para>The shared memory layout is:<br/><br/>
/// <code>
/// | RingBufferHeader | SpscBlockHeader[0] | ... | SpscBlockHeader[N-1] | Block[0] | ... | Block[N-1] |<br/>
/// |                  |                                                | variable |     |  variable  |<br/>
/// |     64 bytes     |                 128 bytes each * N             |------- variable * N  -------|<br/>
/// |--------------------------- Headers -------------------------------|-------- Message data -------|<br/>
/// |----------------------------------- SharedMemorySize(total) -------------------------------------|
/// </code>
/// </para>
/// </remarks>
public unsafe class SpscRingBuffer<TMessageHeader> : IRingBuffer<TMessageHeader>
	where TMessageHeader : unmanaged, IMessageHeader
{
	private const int MinVersionBits = 16;
	private const int MaxBlockSizeBits = 64 - MinVersionBits;
	private const ulong MaxBlockSize = (1UL << MaxBlockSizeBits) - 1;

	private static readonly ulong RingBufferHeaderSize = (ulong)sizeof(RingBufferHeader);
	private static readonly ulong BlockHeaderSize = (ulong)sizeof(SpscBlockHeader);
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
	/// Initializes a new instance of the <see cref="SpscRingBuffer{TMessageHeader}"/> class.
	/// </summary>
	/// <param name="shmPtr">The pointer to the start of the shared memory segment.</param>
	/// <param name="blockCount">The number of blocks in the ring buffer.</param>
	/// <param name="blockSize">The size of each block in bytes.</param>
	/// <param name="isShmemOwner">
	/// A value indicating whether this instance is the owner of the shared memory segment.
	/// This affects whether the ring buffer initializes the memory region.
	/// </param>
	public SpscRingBuffer(byte* shmPtr, uint blockCount, ulong blockSize, bool isShmemOwner)
	{
		ArgumentNullException.ThrowIfNull(shmPtr);

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

		if (isShmemOwner)
		{
			this.SetBlockHeadersToDefault();
			this.SetMessageRegionToDefault();
		}
	}

	~SpscRingBuffer()
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
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public OpStatus Write(TMessageHeader msgHeader, ReadOnlySpan<byte> payload = default)
	{
		if ((this.rbHeaderPtr->Flags & RingBufferFlags.Shutdown) != 0)
		{
			msgHeader = default;
			payload = [];
			return OpStatus.Error;
		}

		ulong entrySize = MessageHeaderSize + (ulong)payload.Length;
		if (entrySize > this.blockSize)
			return OpStatus.Error;

		for (; ; )
		{
			ulong pHead = Volatile.Read(ref this.rbHeaderPtr->ProducerHead);
			ulong blockIdx = this.GetBlockIdx(pHead);
			ulong pVsn = this.GetBlockVsn(pHead);

			SpscBlockHeader* blockHdrPtr = (SpscBlockHeader*)(this.blkHeaderStartPtr + BlockHeaderSize * blockIdx);
			byte* blockPayloadPtr = this.blkPayloadStartPtr + blockIdx * this.blockSize;

			ulong pField = Volatile.Read(ref blockHdrPtr->ProducerField);
			ulong pOff = this.GetCursorOff(pField);

			if (pOff + entrySize <= this.blockSize)
			{
				// Write message header
				*(TMessageHeader*)(blockPayloadPtr + pOff) = msgHeader;

				// Write message payload
				if (payload.Length > 0)
				{
					ref byte src = ref MemoryMarshal.GetReference(payload);
					ref byte dst = ref *(blockPayloadPtr + pOff + MessageHeaderSize);
					Unsafe.CopyBlockUnaligned(ref dst, ref src, (uint)payload.Length);
				}

				// Publish block-level write cursor
				Volatile.Write(ref blockHdrPtr->ProducerField, this.PkgCursor(pOff + entrySize, pVsn));
				return OpStatus.Ok;
			}

			// Block full for this message: pad remaining space if needed
			if (pOff < this.blockSize)
			{
				if (pOff + MessageHeaderSize <= this.blockSize)
				{
					var dummyHeader = new TMessageHeader
					{
						Type = PayloadType.Invalid,
						Length = this.blockSize - pOff - MessageHeaderSize,
					};
					*(TMessageHeader*)(blockPayloadPtr + pOff) = dummyHeader;
				}

				Volatile.Write(ref blockHdrPtr->ProducerField, this.PkgCursor(this.blockSize, pVsn));
			}

			if (this.ProdAdvance())
				continue;

			return OpStatus.Full;
		}
	}

	/// <inheritdoc/>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
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
			ulong blockIdx = this.GetBlockIdx(cHead);
			ulong cVsn = this.GetBlockVsn(cHead);

			SpscBlockHeader* blockHdrPtr = (SpscBlockHeader*)(this.blkHeaderStartPtr + BlockHeaderSize * blockIdx);
			byte* blockPayloadPtr = this.blkPayloadStartPtr + blockIdx * this.blockSize;

			ulong cField = Volatile.Read(ref blockHdrPtr->ConsumerField);
			ulong cOff = this.GetCursorOff(cField);

			if (cOff + MessageHeaderSize <= this.blockSize)
			{
				ulong pField = Volatile.Read(ref blockHdrPtr->ProducerField);
				ulong pOff = this.GetCursorOff(pField);

				if (pOff == cOff)
				{
					msgHeader = default;
					payload = [];
					return OpStatus.Empty;
				}

				// Read message header
				msgHeader = *(TMessageHeader*)(blockPayloadPtr + cOff);
				ulong entrySize = MessageHeaderSize + msgHeader.Length;

				// Handle dummy wrap-around header or invalid/out-of-bounds entry
				if (msgHeader.Type == PayloadType.Invalid || cOff + entrySize > pOff || cOff + entrySize > this.blockSize)
				{
					Volatile.Write(ref blockHdrPtr->ConsumerField, this.PkgCursor(this.blockSize, cVsn));
					if (this.ConsAdvance())
						continue;

					msgHeader = default;
					payload = [];
					return OpStatus.Empty;
				}

				if (msgHeader.Length > 0)
				{
					payload = new ReadOnlySpan<byte>(
						blockPayloadPtr + cOff + MessageHeaderSize,
						(int)msgHeader.Length);
				}
				else
				{
					payload = [];
				}

				// Update block-level consumer cursor
				Volatile.Write(ref blockHdrPtr->ConsumerField, this.PkgCursor(cOff + entrySize, cVsn));
				return OpStatus.Ok;
			}

			if (cOff < this.blockSize)
			{
				Volatile.Write(ref blockHdrPtr->ConsumerField, this.PkgCursor(this.blockSize, cVsn));
			}

			if (this.ConsAdvance())
				continue;

			msgHeader = default;
			payload = [];
			return OpStatus.Empty;
		}
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private bool ProdReady(SpscBlockHeader* block, ulong vsn)
	{
		ulong p = Volatile.Read(ref block->ProducerField);
		return this.GetCursorVsn(p) >= vsn;
	}

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private bool ConsReady(SpscBlockHeader* block, ulong vsn)
	{
		ulong c = Volatile.Read(ref block->ConsumerField);
		ulong cVsn = this.GetCursorVsn(c);
		return (cVsn == vsn && this.GetCursorOff(c) == this.blockSize) || (cVsn > vsn);
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private bool ProdAdvance()
	{
		ulong pHead = Volatile.Read(ref this.rbHeaderPtr->ProducerHead);
		ulong blockIdx = this.GetBlockIdx(pHead);
		ulong pVsn = this.GetBlockVsn(pHead);

		ulong nextIdx = (blockIdx + 1) % this.blockCount;
		ulong nextVsn = pVsn + ((nextIdx == 0) ? 1UL : 0UL);

		SpscBlockHeader* nextBlkHdrPtr = (SpscBlockHeader*)(this.blkHeaderStartPtr + BlockHeaderSize * nextIdx);
		if (!this.ConsReady(nextBlkHdrPtr, nextVsn - 1))
			return false;

		Volatile.Write(ref nextBlkHdrPtr->ProducerField, this.PkgCursor(0, nextVsn));
		Volatile.Write(ref this.rbHeaderPtr->ProducerHead, this.PkgHead(nextIdx, nextVsn));
		return true;
	}

	[MethodImpl(MethodImplOptions.NoInlining)]
	private bool ConsAdvance()
	{
		ulong cHead = Volatile.Read(ref this.rbHeaderPtr->ConsumerHead);
		ulong blockIdx = this.GetBlockIdx(cHead);
		ulong cVsn = this.GetBlockVsn(cHead);

		ulong nextIdx = (blockIdx + 1) % this.blockCount;
		ulong nextVsn = cVsn + ((nextIdx == 0) ? 1UL : 0UL);

		SpscBlockHeader* nextBlkHdrPtr = (SpscBlockHeader*)(this.blkHeaderStartPtr + BlockHeaderSize * nextIdx);
		if (!this.ProdReady(nextBlkHdrPtr, nextVsn))
			return false;

		Volatile.Write(ref nextBlkHdrPtr->ConsumerField, this.PkgCursor(0, nextVsn));
		Volatile.Write(ref this.rbHeaderPtr->ConsumerHead, this.PkgHead(nextIdx, nextVsn));
		return true;
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

		/* Dispose of unmanaged resources here */
		this.shmPtr = null;
		this.rbHeaderPtr = null;
		this.disposedValue = true;
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

	private void SetBlockHeadersToDefault()
	{
		byte* ptr = this.blkHeaderStartPtr;
		byte* endPtr = ptr + BlockHeaderSize * this.rbHeaderPtr->BlockCount;

		// First block (index 0) starts at version 1, offset 0
		*(SpscBlockHeader*)ptr = new SpscBlockHeader
		{
			ProducerField = this.PkgCursor(0, 1),
			ConsumerField = this.PkgCursor(0, 1),
		};

		ptr += BlockHeaderSize;

		// Remaining blocks (index 1..N-1) start at version 0, offset blockSize (ready for producer)
		while (ptr < endPtr)
		{
			*(SpscBlockHeader*)ptr = new SpscBlockHeader
			{
				ProducerField = this.PkgCursor(this.blockSize, 0),
				ConsumerField = this.PkgCursor(this.blockSize, 0),
			};

			ptr += BlockHeaderSize;
		}

		// Initialize ProducerHead and ConsumerHead in RingBufferHeader
		this.rbHeaderPtr->ProducerHead = this.PkgHead(0, 1);
		this.rbHeaderPtr->ConsumerHead = this.PkgHead(0, 1);
	}

	private void SetMessageRegionToDefault()
	{
		// Set the message region to zero
		Unsafe.InitBlockUnaligned(this.blkPayloadStartPtr, 0, (uint)this.blockSize * this.rbHeaderPtr->BlockCount);
	}
}
