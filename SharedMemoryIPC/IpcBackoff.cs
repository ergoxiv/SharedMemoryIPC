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

/// <summary>
/// High-performance, zero-allocation backoff strategy for lock-free IPC operations.
/// </summary>
/// <remarks>
/// Based on Agrona's idle backoff strategy: spin -> yield -> park the thread for kernel-level waiting.
/// </remarks>
[StructLayout(LayoutKind.Sequential, Pack = 4)]
public struct IpcBackoff
{
	private const int ActiveSpinThreshold = 10;
	private const int YieldThreshold = 20;
	private static readonly bool IsMultiProcessor = Environment.ProcessorCount > 1;

	private int stepCount;
	private int pause;

	/// <summary>
	/// Creates a new backoff object instance.
	/// </summary>
	public IpcBackoff()
	{
		this.stepCount = IsMultiProcessor ? 0 : ActiveSpinThreshold;
		this.pause = 1;
	}

	/// <summary>
	/// Executes one step of the backoff sequence.
	/// </summary>
	/// <returns>
	/// True if still in user-space spin/yield phase; false if ready for kernel wait.
	/// </returns>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public bool Step()
	{
		this.stepCount++;

		if (this.stepCount <= ActiveSpinThreshold)
		{
			Thread.SpinWait(this.pause);

			// Exponential backoff
			if (this.pause < 32)
				this.pause <<= 1;

			return true;
		}

		if (this.stepCount <= YieldThreshold)
		{
			Thread.Yield();
			return true;
		}

		return false;
	}

	/// <summary>
	/// Resets the backoff counter for a new wait cycle.
	/// </summary>
	[MethodImpl(MethodImplOptions.AggressiveInlining)]
	public void Reset()
	{
		this.stepCount = IsMultiProcessor ? 0 : ActiveSpinThreshold;
		this.pause = 1;
	}
}
