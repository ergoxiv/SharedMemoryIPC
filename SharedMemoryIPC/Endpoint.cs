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
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Runtime.InteropServices;
using System.Threading;

namespace SharedMemoryIPC;

public class Endpoint
	: Endpoint<MessageHeader>
{
	public Endpoint(string shmFilename, uint blockCount, ulong blockSize)
		: base(shmFilename, blockCount, blockSize)
	{
	}

	public Endpoint(string shmFilename)
		: base(shmFilename)
	{
	}

	public ulong Write<T>(uint id, T payload, int timeoutMs = 1000)
		where T : unmanaged
	{
		PayloadType type = GetPayloadType<T>();
		ulong payloadSize;
		byte[] payloadBytes;

		if (type == PayloadType.Blob)
		{
			payloadSize = (ulong)Marshal.SizeOf<T>();
			payloadBytes = MarshalToByteArray(payload, typeof(T));
		}
		else
		{
			payloadSize = (ulong)Marshal.SizeOf<T>();
			payloadBytes = new byte[payloadSize];
			MemoryMarshal.Write(payloadBytes, in payload);
		}

		var header = new MessageHeader
		{
			Id = id,
			Type = type,
			Length = payloadSize,
		};

		if (!this.Write(header, payloadBytes, timeoutMs))
			return 0;

		return payloadSize;
	}

	public bool Read<T>(out uint id, out T payload, int timeoutMs = 1000)
		where T : unmanaged
	{
		id = 0;
		payload = default;

		PayloadType expectedType = GetPayloadType<T>();
		ulong expectedSize = (ulong)Marshal.SizeOf<T>();

		if (!this.Read(out MessageHeader header, out byte[] payloadBytes, timeoutMs))
			return false;

		if (header.Type != expectedType)
			throw new InvalidOperationException($"Payload type mismatch. Expected {expectedType}, but got {header.Type}.");

		if (header.Length != expectedSize)
			throw new InvalidOperationException($"Payload size mismatch. Expected {expectedSize} bytes, but got {header.Length} bytes.");

		id = header.Id;
		var payloadSpan = new ReadOnlySpan<byte>(payloadBytes);
		payload = MemoryMarshal.Read<T>(payloadSpan);

		return true;
	}

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

	// Yoinked from Anamnesis' MemoryService
	private static byte[] MarshalToByteArray(object value, Type type)
	{
		int size = Marshal.SizeOf(type);
		byte[] buffer = new byte[size];
		IntPtr mem = Marshal.AllocHGlobal(size);
		try
		{
			Marshal.StructureToPtr(value, mem, false);
			Marshal.Copy(mem, buffer, 0, size);
		}
		catch (Exception ex)
		{
			throw new Exception($"Failed to marshal type: {type} to memory", ex);
		}
		finally
		{
			Marshal.FreeHGlobal(mem);
		}

		return buffer;
	}
}

// TODO: Endpoint class should have a helper that constructs the message header + payload in a single contiguous memory block, so users don't have to do this manually every time.

public unsafe class Endpoint<TMessageHeader> : IDisposable
	where TMessageHeader : unmanaged, IMessageHeader
{
	private bool desiredShmOwner;
	private ulong sharedMemorySize; // TODO: Avoid calculating this multiple times (in the endpoint and in the ring buffer)
	private uint blockCount;
	private ulong blockSize;
	private bool disposedValue;

	private EventWaitHandle? canReadEvent = null;  // Signaled after a write to indicate data is available to read
	private EventWaitHandle? canWriteEvent = null; // Signaled after a read to indicate space is available to write

	private RingBuffer<TMessageHeader>? ringBuffer = null;
	private MemoryMappedFile? mmf = null;
	private MemoryMappedViewAccessor? accessor = null;
	private byte* shmPtr = null;

	// This constructor be used by users who do not mind which endpoint is the owner of the shared memory segment
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

	// To be used by users who enforce non-ownership on this endpoint
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

	public string Name { get; private set; }

	public void Dispose()
	{
		this.Dispose(true);
		GC.SuppressFinalize(this);
	}

	private void Dispose(bool disposing)
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

	public bool Write(TMessageHeader header, ReadOnlySpan<byte> payload, int timeoutMs = 1000)
	{
		if ((ulong)payload.Length > this.blockSize)
			throw new ArgumentOutOfRangeException(nameof(payload), "Payload size exceeds block size.");

		if (timeoutMs <= 0)
			throw new ArgumentException("Timeout must be greater than zero.", nameof(timeoutMs));

		while (true)
		{
			OpStatus status = this.ringBuffer!.Write(header, payload.ToArray());

			if (status == OpStatus.Ok)
			{
				this.canReadEvent!.Set();
				return true;
			}

			if (!this.canWriteEvent!.WaitOne(timeoutMs))
			{
				Console.WriteLine("Write timed out");
				return false;
			}
		}
	}

	public bool Read(out TMessageHeader header, out byte[] payload, int timeoutMs = 1000)
	{
		if (timeoutMs <= 0)
			throw new ArgumentException("Timeout must be greater than zero.", nameof(timeoutMs));

		while (true)
		{
			OpStatus status = this.ringBuffer!.Read(out header, out payload);
			if (status == OpStatus.Ok)
			{
				this.canWriteEvent!.Set();
				return true;
			}

			if (!this.canReadEvent!.WaitOne(timeoutMs))
			{
				Console.WriteLine("Read timed out");
				return false;
			}
		}
	}

	private void OpenMemoryMappedFile()
	{
		// Close any existing MMF connections if there are any
		this.CloseMemoryMappedFile();

		if (this.sharedMemorySize > long.MaxValue)
			throw new ArgumentOutOfRangeException(nameof(this.sharedMemorySize), "Shared memory size exceeds maximum allowed size.");

		if (!OperatingSystem.IsWindows())
			throw new PlatformNotSupportedException($"{nameof(MemoryMappedFile.CreateOrOpen)} is only supported on Windows.");

		using var mmfMutex = new Mutex(false, $"Global\\{this.Name}_MMF_Mutex");
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
					while (true)
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
							Thread.Sleep(pollIntervalMs);
							waited += pollIntervalMs;
						}
					}
				}


				//if (!this.desiredShmOwner)
				//	throw;

				//this.mmf = MemoryMappedFile.CreateNew(this.Name, (long)this.sharedMemorySize, MemoryMappedFileAccess.ReadWrite);
				//isTrueOwner = true;
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
			this.canReadEvent = new EventWaitHandle(false, EventResetMode.AutoReset, $"{this.Name}_Ev_CanRead");
			this.canWriteEvent = new EventWaitHandle(false, EventResetMode.AutoReset, $"{this.Name}_Ev_CanWrite");
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

	private void CloseMemoryMappedFile()
	{
		this.canReadEvent?.Dispose();
		this.canReadEvent = null;
		this.canWriteEvent?.Dispose();
		this.canWriteEvent = null;

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
}
