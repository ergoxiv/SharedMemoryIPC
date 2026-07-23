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

namespace SharedMemoryIPC;

/// <summary>
/// An interface for defining message headers used in the ring buffer.
/// </summary>
public interface IMessageHeader
{
	/// <summary>
	/// Gets or sets the type of payload represented by this instance.
	/// </summary>
	PayloadType Type { get; set; }

	/// <summary>
	/// Gets or sets the total length, in bytes, of the data or stream.
	/// </summary>
	ulong Length { get; set; }
}
