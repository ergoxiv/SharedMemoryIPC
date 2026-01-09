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
using System.ComponentModel;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading;

namespace SharedMemoryIPC;

/// <inheritdoc/>
public class Endpoint : Endpoint<MessageHeader>
{
	/// <inheritdoc/>
	public Endpoint(string shmFilename, uint blockCount, ulong blockSize)
		: base(shmFilename, blockCount, blockSize)
	{
	}

	/// <inheritdoc/>
	public Endpoint(string shmFilename)
		: base(shmFilename)
	{
	}

	/// <summary>
	/// Writes a typed message to the shared memory segment.
	/// </summary>
	/// <typeparam name="T">The type of the payload to write.</typeparam>
	/// <param name="id">The message identifier.</param>
	/// <param name="payload">The message payload.</param>
	/// <param name="timeoutMs">
	/// The timeout in milliseconds to wait for space to become available.
	/// </param>
	/// <returns>
	/// The number of bytes written if the message was written successfully; otherwise, zero.
	/// </returns>
	public int Write<T>(uint id, T payload, uint timeoutMs = 1000)
		where T : unmanaged
	{
		PayloadType type = GetPayloadType<T>();
		int payloadSize = Marshal.SizeOf<T>();

		Span<byte> payloadSpan = payloadSize <= 256
			? stackalloc byte[payloadSize]
			: new byte[payloadSize];

		MemoryMarshal.Write(payloadSpan, in payload);

		var header = new MessageHeader
		{
			Id = id,
			Type = type,
			Length = (ulong)payloadSize,
		};

		if (!this.Write(header, payloadSpan, timeoutMs))
			return 0;

		return payloadSize;
	}

	/// <summary>
	/// Writes a header-only message to the shared memory segment.
	/// </summary>
	/// <param name="header">The message header.</param>
	/// <param name="timeoutMs">
	/// The timeout in milliseconds to wait for space to become available.
	/// </param>
	/// <returns>
	/// True if the message was written successfully; otherwise, false.
	/// </returns>
	public bool Write(MessageHeader header, uint timeoutMs = 1000) => this.Write(header, [], timeoutMs);

	/// <summary>
	/// Reads a typed message from the shared memory segment.
	/// </summary>
	/// <typeparam name="T">The type of the payload to read.</typeparam>s
	/// <param name="id">The output message identifier.</param>
	/// <param name="payload">The output message payload.</param>
	/// <param name="timeoutMs">
	/// The timeout in milliseconds to wait for data to become available.
	/// </param>
	/// <returns>
	/// True if a message was read successfully; otherwise, false.
	/// </returns>
	/// <exception cref="InvalidOperationException">
	/// Thrown when the payload type or size does not match the expected type or size.
	/// </exception>
	public bool Read<T>(out uint id, out T payload, uint timeoutMs = 1000)
		where T : unmanaged
	{
		id = 0;
		payload = default;

		PayloadType expectedType = GetPayloadType<T>();
		int expectedSize = Marshal.SizeOf<T>();

		if (!this.Read(out MessageHeader header, out ReadOnlySpan<byte> payloadSpan, timeoutMs))
			return false;

		if (header.Type != expectedType)
			throw new InvalidOperationException($"Payload type mismatch. Expected {expectedType}, but got {header.Type}.");

		if (header.Length != (ulong)expectedSize)
			throw new InvalidOperationException($"Payload size mismatch. Expected {expectedSize} bytes, but got {header.Length} bytes.");

		id = header.Id;
		payload = MemoryMarshal.Read<T>(payloadSpan);

		return true;
	}

	/// <summary>
	/// Reads a header-only message from the shared memory segment.
	/// </summary>
	/// <param name="header">The output message header.</param>
	/// <param name="timeoutMs">
	/// The timeout in milliseconds to wait for data to become available.
	/// </param>
	/// <returns>
	/// True if a message was read successfully; otherwise, false.
	/// </returns>
	public bool Read(out MessageHeader header, uint timeoutMs = 1000) => this.Read(out header, out var _, timeoutMs);

	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	private static PayloadType GetPayloadType<T>()
	{
		Type t = typeof(T);

		return t switch
		{
			_ when t == typeof(bool) => PayloadType.Bit,
			_ when t == typeof(sbyte) => PayloadType.Int8,
			_ when t == typeof(byte) => PayloadType.UInt8,
			_ when t == typeof(short) => PayloadType.Int16,
			_ when t == typeof(ushort) => PayloadType.UInt16,
			_ when t == typeof(int) => PayloadType.Int32,
			_ when t == typeof(uint) => PayloadType.UInt32,
			_ when t == typeof(long) => PayloadType.Int64,
			_ when t == typeof(ulong) => PayloadType.UInt64,
			_ when t == typeof(Half) => PayloadType.Float16,
			_ when t == typeof(float) => PayloadType.Float32,
			_ when t == typeof(double) => PayloadType.Float64,
			_ when t == typeof(Guid) => PayloadType.Guid,
			_ => PayloadType.Blob,
		};
	}
}


/// <summary>
/// A shared memory inter-process communication (IPC) endpoint
/// based on an internal high-performance ring buffer.
/// </summary>
/// <typeparam name="TMessageHeader">
/// The type of the message header used for communication.
/// </typeparam>
public unsafe class Endpoint<TMessageHeader> : IDisposable
	where TMessageHeader : unmanaged, IMessageHeader
{
	protected const int SpinWaitIterations = 100;
	protected const int SpinWaitDelay = 10;

	private const uint WAIT_OBJECT_0 = 0x00000000;
	private static readonly IntPtr INVALID_HANDLE_VALUE = new(-1);

	private readonly bool desiredShmOwner;
	private ulong sharedMemorySize;
	private uint blockCount;
	private ulong blockSize;
	private bool disposedValue;

	private IntPtr canReadEvent = IntPtr.Zero;  // Signaled after a write to indicate data is available to read
	private IntPtr canWriteEvent = IntPtr.Zero; // Signaled after a read to indicate space is available to write

	private RingBuffer<TMessageHeader>? ringBuffer = null;
	private MemoryMappedFile? mmf = null;
	private MemoryMappedViewAccessor? accessor = null;
	private byte* shmPtr = null;

	/// <summary>
	/// Initializes a new instance of the <see cref="Endpoint{TMessageHeader}"/> class.
	/// This is intended to be used by callers who do not mind which
	/// endpoint is the owner of the shared memory segment.
	/// </summary>
	/// <param name="shmFilename">The name of the shared memory segment.</param>
	/// <param name="blockCount">The number of blocks in the ring buffer.</param>
	/// <param name="blockSize">The size of each block in bytes.</param>
	public Endpoint(string shmFilename, uint blockCount, ulong blockSize)
	{
		ArgumentException.ThrowIfNullOrEmpty(shmFilename, nameof(shmFilename));
		ArgumentOutOfRangeException.ThrowIfZero(blockCount, nameof(blockCount));
		ArgumentOutOfRangeException.ThrowIfZero(blockSize, nameof(blockSize));

		if (blockSize % 8 != 0)
			Console.WriteLine("Warning: It is recommended that block size be a multiple of 8 bytes for optimal alignment.");

		this.Name = shmFilename;
		this.desiredShmOwner = true;
		this.blockCount = blockCount;
		this.blockSize = blockSize;
		this.sharedMemorySize = (ulong)Marshal.SizeOf<RingBufferHeader>() + blockCount * ((ulong)Marshal.SizeOf<BlockHeader>() + blockSize);

		this.OpenMemoryMappedFile();
	}

	/// <summary>
	/// Initializes a new instance of the <see cref="Endpoint{TMessageHeader}"/> class.
	/// This is intended to be used by callers who wish to enforce non-ownership
	/// on this endpoint.
	/// </summary>
	/// <param name="shmFilename">The name of the shared memory segment.</param>
	public Endpoint(string shmFilename)
	{
		ArgumentException.ThrowIfNullOrEmpty(shmFilename);

		this.Name = shmFilename;
		this.desiredShmOwner = false;
		this.blockCount = 0;
		this.blockSize = 0;
		this.sharedMemorySize = 0;

		this.OpenMemoryMappedFile();
	}

	~Endpoint()
	{
		this.Dispose(false);
	}

	/// <summary>
	/// Gets the name of the shared memory segment.
	/// </summary>
	public string Name { get; private set; }

	/// <summary>
	/// Disposes the endpoint and releases all associated resources.
	/// </summary>
	public void Dispose()
	{
		this.Dispose(true);
		GC.SuppressFinalize(this);
	}

	/// <summary>
	/// Writes a message to the shared memory segment.
	/// </summary>
	/// <param name="header">The message header.</param>
	/// <param name="payload">The message payload.</param>
	/// <param name="timeoutMs">
	/// The timeout in milliseconds to wait for space to become available.
	/// </param>
	/// <returns>
	/// True if the message was written successfully; otherwise, false.
	/// In other words, false indicates either a timeout or full ring buffer.
	/// </returns>
	/// <exception cref="ArgumentOutOfRangeException">
	/// Thrown when the payload size exceeds the block size.
	/// </exception>
	/// <exception cref="ArgumentException">
	/// Thrown when the timeout is less than or equal to zero.
	/// </exception>
	public bool Write(TMessageHeader header, ReadOnlySpan<byte> payload, uint timeoutMs = 1000)
	{
		if ((ulong)payload.Length > this.blockSize)
			throw new ArgumentOutOfRangeException(nameof(payload), "Payload size exceeds block size.");

		if (timeoutMs <= 0)
			throw new ArgumentException("Timeout must be greater than zero.", nameof(timeoutMs));

		var rb = this.ringBuffer!;
		var readEvent = this.canReadEvent;
		var writeEvent = this.canWriteEvent;

		int i = 0;
		OpStatus status;
		for (; ; )
		{
			if (rb == null)
				return false;

			status = rb.Write(header, payload);
			if (status == OpStatus.Ok)
			{
				NativeMethods.SetEvent(readEvent);
				return true;
			}

			for (; i < SpinWaitIterations; i++)
			{
				Thread.SpinWait(SpinWaitDelay);
				status = rb.Write(header, payload);
				if (status == OpStatus.Ok)
				{
					NativeMethods.SetEvent(readEvent);
					return true;
				}
			}

			if (NativeMethods.WaitForSingleObject(writeEvent, timeoutMs) != WAIT_OBJECT_0)
				return false;
		}
	}

	/// <summary>
	/// Reads a message from the shared memory segment.
	/// </summary>
	/// <param name="header">The output message header.</param>
	/// <param name="payload">The output message payload.</param>
	/// <param name="timeoutMs">
	/// The timeout in milliseconds to wait for data to become available.
	/// </param>
	/// <returns>
	/// True if a message was read successfully; otherwise, false.
	/// In other words, false indicates either a timeout or empty ring buffer.
	/// </returns>
	/// <exception cref="ArgumentException">
	/// Thrown when the timeout is less than or equal to zero.
	/// </exception>
	public bool Read(out TMessageHeader header, out ReadOnlySpan<byte> payload, uint timeoutMs = 1000)
	{
		if (timeoutMs <= 0)
			throw new ArgumentException("Timeout must be greater than zero.", nameof(timeoutMs));

		var rb = this.ringBuffer;
		var readEvent = this.canReadEvent;
		var writeEvent = this.canWriteEvent;

		int i = 0;
		OpStatus status;
		for (; ; )
		{
			if (rb == null)
			{
				header = default;
				payload = default;
				return false;
			}

			status = rb.Read(out header, out payload);
			if (status == OpStatus.Ok)
			{
				NativeMethods.SetEvent(writeEvent);
				return true;
			}

			for (; i < SpinWaitIterations; i++)
			{
				Thread.SpinWait(SpinWaitDelay);
				status = rb.Read(out header, out payload);
				if (status == OpStatus.Ok)
				{
					NativeMethods.SetEvent(writeEvent);
					return true;
				}
			}

			if (NativeMethods.WaitForSingleObject(readEvent, timeoutMs) != WAIT_OBJECT_0)
				return false;
		}
	}

	protected void OpenMemoryMappedFile()
	{
		// Close any existing MMF connections if there are any
		this.CloseMemoryMappedFile();

		if (this.sharedMemorySize > long.MaxValue)
			throw new ArgumentOutOfRangeException(nameof(this.sharedMemorySize), "Shared memory size exceeds maximum allowed size.");

		if (!OperatingSystem.IsWindows())
			throw new PlatformNotSupportedException($"{nameof(MemoryMappedFile.CreateOrOpen)} is only supported on Windows.");

		using var mmfMutex = new Mutex(false, $"{this.Name}_MMF_Mutex");
		mmfMutex.WaitOne();
		try
		{
			bool isTrueOwner = false;

			// Determine if this endpoint is the true owner (creator) of the shared memory segment
			try
			{
				this.mmf = MemoryMappedFile.OpenExisting(this.Name, MemoryMappedFileRights.ReadWrite);
			}
			catch (FileNotFoundException)
			{
				if (this.desiredShmOwner)
				{
					this.mmf = MemoryMappedFile.CreateNew(this.Name, (long)this.sharedMemorySize, MemoryMappedFileAccess.ReadWrite);
					isTrueOwner = true;
				}
				else
				{
					// Wait for the file to be created by the owner
					const int maxWaitMs = 60000;
					const int pollIntervalMs = 50;
					int waited = 0;
					for (; ; )
					{
						try
						{
							this.mmf = MemoryMappedFile.OpenExisting(this.Name, MemoryMappedFileRights.ReadWrite);
							break;
						}
						catch (FileNotFoundException)
						{
							if (waited >= maxWaitMs)
								throw new TimeoutException($"Timeout waiting for shared memory region '{this.Name}' to be created.");

							mmfMutex.ReleaseMutex();
							Thread.Sleep(pollIntervalMs);
							mmfMutex.WaitOne();
							waited += pollIntervalMs;
						}
					}
				}
			}

			if (isTrueOwner)
			{
				this.accessor = this.mmf.CreateViewAccessor(0, (long)this.sharedMemorySize, MemoryMappedFileAccess.ReadWrite);
				this.accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref this.shmPtr);

				if (this.shmPtr == null)
					throw new InvalidOperationException("Failed to acquire pointer to shared memory.");

				// Initialize the ring buffer header
				var rbHeader = new RingBufferHeader
				{
					SharedMemorySize = this.sharedMemorySize,
					BlockCount = this.blockCount,
					BlockSize = this.blockSize,
					ProducerHead = 0,
					ConsumerHead = 0,
					Flags = RingBufferFlags.None,
				};
				this.accessor.Write(0, ref rbHeader);
				this.ringBuffer = new RingBuffer<TMessageHeader>(this.shmPtr, this.blockCount, this.blockSize, isTrueOwner);
			}
			else
			{
				using (var headerView = this.mmf.CreateViewAccessor(0, Marshal.SizeOf<RingBufferHeader>(), MemoryMappedFileAccess.Read))
				{
					byte* headerPtr = null;
					headerView.SafeMemoryMappedViewHandle.AcquirePointer(ref headerPtr);
					var header = (RingBufferHeader*)headerPtr;
					this.sharedMemorySize = header->SharedMemorySize;
					this.blockCount = header->BlockCount;
					this.blockSize = header->BlockSize;
					headerView.SafeMemoryMappedViewHandle.ReleasePointer();
				}

				this.accessor = this.mmf.CreateViewAccessor(0, (long)this.sharedMemorySize, MemoryMappedFileAccess.ReadWrite);
				this.accessor.SafeMemoryMappedViewHandle.AcquirePointer(ref this.shmPtr);

				if (this.shmPtr == null)
					throw new InvalidOperationException("Failed to acquire pointer to shared memory.");

				this.ringBuffer = new RingBuffer<TMessageHeader>(this.shmPtr, this.blockCount, this.blockSize, isTrueOwner);
			}

			// Create the sync events
			this.canReadEvent = CreateOrOpenEvent($"{this.Name}_Ev_CanRead");
			this.canWriteEvent = CreateOrOpenEvent($"{this.Name}_Ev_CanWrite");
		}
		catch
		{
			this.CloseMemoryMappedFile();
			throw;
		}
		finally
		{
			mmfMutex.ReleaseMutex();
		}
	}

	protected void CloseMemoryMappedFile()
	{
		if (this.canReadEvent != IntPtr.Zero)
		{
			NativeMethods.CloseHandle(this.canReadEvent);
			this.canReadEvent = IntPtr.Zero;
		}
		if (this.canWriteEvent != IntPtr.Zero)
		{
			NativeMethods.CloseHandle(this.canWriteEvent);
			this.canWriteEvent = IntPtr.Zero;
		}

		// Dispose of the ring buffer
		// NOTE: If the endpoint is the owner of the shared memory, ring buffer
		// disposal will mark the shared memory segment as closed/shut down.
		if (this.accessor != null)
		{
			this.ringBuffer?.Dispose();
			this.ringBuffer = null;
		}

		this.accessor?.SafeMemoryMappedViewHandle.ReleasePointer();
		this.shmPtr = null;
		this.accessor?.Dispose();
		this.accessor = null;
		this.mmf?.Dispose();
		this.mmf = null;
	}

	protected static IntPtr CreateOrOpenEvent(string name)
	{
		var handle = NativeMethods.CreateEvent(IntPtr.Zero, false, false, name);
		if (handle == IntPtr.Zero || handle == INVALID_HANDLE_VALUE)
			throw new Win32Exception(Marshal.GetLastWin32Error(), $"Failed to create/open event '{name}'");
		return handle;
	}

	protected void Dispose(bool disposing)
	{
		if (this.disposedValue)
			return;

		if (disposing)
		{
			/* Dispose of managed resources here */
			this.CloseMemoryMappedFile();
		}

		/* Dipose of unmanaged resources here */
		this.disposedValue = true;
	}
}