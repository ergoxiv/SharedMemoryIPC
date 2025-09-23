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

using SharedMemoryIPC;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.InteropServices;

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct BenchmarkPayload
{
	public long Timestamp;
	public int Sequence;
}

public static class Benchmark
{
	const string ShmNameC2S = "BenchmarkIPC_C2S";
	const string ShmNameS2C = "BenchmarkIPC_S2C";
	const uint BlockCount = 16; // 128
	const ulong BlockSize = 4096; // 8192
	const int MessageCount = 100_000;

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

	public static void RunClient()
	{
		using var endpointC2S = new Endpoint(ShmNameC2S);
		using var endpointS2C = new Endpoint(ShmNameS2C);
		long totalLatencyTicks = 0;
		int successful = 0;
		var latencySamples = new List<long>();
		int sampleRate = 1000; // Record every 1000th latency sample

		var sw = Stopwatch.StartNew();
		for (int i = 0; i < MessageCount; i++)
		{
			var payload = new BenchmarkPayload
			{
				Timestamp = Stopwatch.GetTimestamp(),
				Sequence = i,
			};

			endpointC2S.Write((uint)i, payload, 1000);

			if (endpointS2C.Read<BenchmarkPayload>(out uint id, out var reply, 1000))
			{
				long now = Stopwatch.GetTimestamp();
				long latencyTicks = now - reply.Timestamp;
				totalLatencyTicks += latencyTicks;
				successful++;

				if (successful % 1000 == 0)
				{
					latencySamples.Add(latencyTicks);
				}
			}
		}
		sw.Stop();

		double avgLatencyUs = (totalLatencyTicks * 1_000_000.0) / (Stopwatch.Frequency * successful);
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

			Console.WriteLine($"Latency (rate: 1/{sampleRate}): avg={avgSampleUs:F2} us, min={minSampleUs:F2} us, max={maxSampleUs:F2} us");
		}

		Console.WriteLine($"Throughput: {roundTripThroughput:F2} round-trip sequences/sec");
		Console.WriteLine($"Throughput: {messageThroughput:F2} messages/sec (both directions)");
		Console.WriteLine($"Throughput: {mbPerSec:F2} MiB/sec (both directions)");
	}
}