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
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;

namespace SharedMemoryIPC;

/// <summary>
/// An example payload structure for benchmarking.
/// </summary>
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public unsafe struct BenchmarkPayload
{
	public long Timestamp;
	public uint Sequence;
	public fixed byte _Padding[4084]; // Padding to make the struct 4KiB
}

/// <summary>
/// An example benchmark for measuring throughput and latency
/// between IPC endpoints (client and server).
/// </summary>
public static class Benchmark
{
	const string ShmNameC2S = "BenchmarkIPC_C2S";
	const string ShmNameS2C = "BenchmarkIPC_S2C";
	const uint BlockCount = 128;
	const ulong BlockSize = 8192;
	const int MessageCount = 1_000_000;
	const int LatencySampleRate = 1000;
	const int LatencySampleCount = MessageCount / LatencySampleRate;

	/// <summary>
	/// Example server that echoes back received messages.
	/// </summary>
	public static void RunServer()
	{
		using var endpointC2S = new Endpoint(ShmNameC2S, BlockCount, BlockSize);
		using var endpointS2C = new Endpoint(ShmNameS2C, BlockCount, BlockSize);
		int received = 0;
		while (received < MessageCount)
		{
			if (endpointC2S.Read<BenchmarkPayload>(out uint id, out var payload, 1000))
			{
				// Echo back with server's sender ID
				endpointS2C.Write(id, payload, 1000);
				received++;
			}
		}
		Console.WriteLine($"Server: Processed {received} messages.");
	}

	/// <summary>
	/// Example client that sends messages and measures
	/// throughput and latency on round-trip communication.
	/// </summary>
	public static void RunClient()
	{
		using var endpointC2S = new Endpoint(ShmNameC2S);
		using var endpointS2C = new Endpoint(ShmNameS2C);
		var latencySamples = new List<long>(LatencySampleCount);
		int successful = 0;

		var sw = Stopwatch.StartNew();
		for (uint i = 0; i < MessageCount; i++)
		{
			var payload = new BenchmarkPayload
			{
				Timestamp = Stopwatch.GetTimestamp(),
				Sequence = i,
			};

			endpointC2S.Write(i, payload, 1000);

			if (endpointS2C.Read<BenchmarkPayload>(out uint id, out var reply, 1000))
			{
				successful++;

				if (successful % LatencySampleRate == 0)
					latencySamples.Add(Stopwatch.GetTimestamp() - reply.Timestamp);
			}
		}
		sw.Stop();

		double roundTripThroughput = successful / sw.Elapsed.TotalSeconds;
		double messageThroughput = roundTripThroughput * 2;

		int payloadSize = Marshal.SizeOf<BenchmarkPayload>();
		double mbPerSec = (messageThroughput * payloadSize) / (1024 * 1024);

		Console.WriteLine($"Client: Sent/Received {successful} messages.");

		if (latencySamples.Count > 0)
		{
			latencySamples.Sort();
			double minSampleUs = (latencySamples[0] * 1_000_000.0) / Stopwatch.Frequency;
			double maxSampleUs = (latencySamples[^1] * 1_000_000.0) / Stopwatch.Frequency;
			double avgSampleUs = (latencySamples.Average() * 1_000_000.0) / Stopwatch.Frequency;

			Console.WriteLine($"Latency (rate: 1/{LatencySampleRate}): avg={avgSampleUs:F2} us, min={minSampleUs:F2} us, max={maxSampleUs:F2} us");
		}

		Console.WriteLine($"Throughput: {roundTripThroughput:F2} round-trip sequences/sec");
		Console.WriteLine($"Throughput: {messageThroughput:F2} messages/sec");
		Console.WriteLine($"Throughput: {mbPerSec:F2} MiB/sec");
	}
}