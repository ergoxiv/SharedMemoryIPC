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

public static partial class NativeMethods
{
	[LibraryImport("kernel32.dll", SetLastError = true, StringMarshalling = StringMarshalling.Utf16, EntryPoint = "CreateEventW")]
	public static partial IntPtr CreateEvent(
		IntPtr lpEventAttributes,
		[MarshalAs(UnmanagedType.Bool)] bool bManualReset,
		[MarshalAs(UnmanagedType.Bool)] bool bInitialState,
		string lpName);

	[LibraryImport("kernel32.dll", SetLastError = false)]
	[return: MarshalAs(UnmanagedType.Bool)]
	public static partial bool SetEvent(IntPtr hEvent);

	[LibraryImport("kernel32.dll", SetLastError = false)]
	internal static partial uint WaitForSingleObject(IntPtr hHandle, uint dwMilliseconds);

	[LibraryImport("kernel32.dll", SetLastError = false)]
	[return: MarshalAs(UnmanagedType.Bool)]
	public static partial bool CloseHandle(IntPtr hObject);
}
