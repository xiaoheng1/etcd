// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ioutil

import (
	"io"
)

var defaultBufferBytes = 128 * 1024

// PageWriter implements the io.Writer interface so that writes will
// either be in page chunks or from flushing.
type PageWriter struct {
	w io.Writer
	// pageOffset tracks the page offset of the base of the buffer
	// 一页中的偏移量.
	pageOffset int
	// pageBytes is the number of bytes per page
	pageBytes int
	// bufferedBytes counts the number of bytes pending for write in the buffer
	bufferedBytes int
	// buf holds the write buffer
	// buf 保存写缓冲区
	buf []byte
	// bufWatermarkBytes is the number of bytes the buffer can hold before it needs
	// to be flushed. It is less than len(buf) so there is space for slack writes
	// to bring the writer to page alignment.
	// bufWatermarkBytes 是缓冲区在需要刷新前可以容纳的字节数. 它小于 len(buf) 因此
	// 有空间用于松弛写入以使写入器进行对齐.
	bufWatermarkBytes int
}

// NewPageWriter creates a new PageWriter. pageBytes is the number of bytes
// to write per page. pageOffset is the starting offset of io.Writer.
func NewPageWriter(w io.Writer, pageBytes, pageOffset int) *PageWriter {
	return &PageWriter{
		w:          w,
		pageOffset: pageOffset,
		pageBytes:  pageBytes,
		// 可能是为了对齐.
		// TODO: 待确定
		buf:               make([]byte, defaultBufferBytes+pageBytes),
		bufWatermarkBytes: defaultBufferBytes,
	}
}

// 其实这个方法，最最核心的是对缓冲区的数据进行处理. 核心思路就是 buf(缓冲区) 对齐后就往磁盘中刷新.
// 1.当写入数据 + 缓存小于水位的时候，直接写入
// 2.当写入数据 + 缓存大于水位的是你，分多种情况：
//  (1.1) 如果当前缓冲区未对齐，且未对齐容量 > 当前写入数据，写入缓冲区.
//  (1.2) 如果当前缓冲区未对齐，且未对齐容量 <= 当前写入数据，写入缓冲区. 这时候会将 p 的一部分数据拷贝到缓冲区.
// 3.不论是那种情况，缓冲区都是对齐的，直接刷新.

// 补充：
// Buffer IO 大多数文件系统默认 IO 都是 BufferIO. 在 Linux 的 Buffer IO 机制中，
// 操作系统会将 IO 的数据缓存在文件系统的页缓存(page cache) 中.
// 例如 FileInputStream/FileOutputStream/RandomAccessFile/FileChannel.

// 直接写 PageCache MMP.

// 为什么要为某个文件快速地分配固定大小的磁盘空间？
// 1.可以让文件尽可能的占用连续的磁盘扇区，减少后续写入和读取文件时的磁盘训道开销.
// 2.迅速占用磁盘空间，防止使用过程中所需空间不足.
func (pw *PageWriter) Write(p []byte) (n int, err error) {
	// 当前缓存的字节数  + 待写入的字节数 小于最高水位，说明是安全的
	// 直接写入.
	if len(p)+pw.bufferedBytes <= pw.bufWatermarkBytes {
		// no overflow
		copy(pw.buf[pw.bufferedBytes:], p)
		pw.bufferedBytes += len(p)
		return len(p), nil
	}
	// complete the slack page in the buffer if unaligned
	// len(p) + bufferedBytes > bufWatermarkBytes 超过了水位上限.
	// 如果未对齐，则完成缓冲区中的松弛页面.
	slack := pw.pageBytes - ((pw.pageOffset + pw.bufferedBytes) % pw.pageBytes)
	// 不想等，slack 则是需要被填充的部分.
	if slack != pw.pageBytes {
		partial := slack > len(p)
		if partial {
			// not enough data to complete the slack page
			slack = len(p)
		}
		// special case: writing to slack page in buffer
		copy(pw.buf[pw.bufferedBytes:], p[:slack])
		pw.bufferedBytes += slack
		n = slack
		p = p[slack:]
		if partial {
			// avoid forcing an unaligned flush
			// 避免强制未对齐的刷新.
			return n, nil
		}
	}
	// buffer contents are now page-aligned; clear out
	if err = pw.Flush(); err != nil {
		return n, err
	}
	// directly write all complete pages without copying
	// 如果满足对齐，则直接写入.
	if len(p) > pw.pageBytes {
		pages := len(p) / pw.pageBytes
		c, werr := pw.w.Write(p[:pages*pw.pageBytes])
		n += c
		if werr != nil {
			return n, werr
		}
		p = p[pages*pw.pageBytes:]
	}
	// write remaining tail to buffer
	c, werr := pw.Write(p)
	n += c
	return n, werr
}

// Flush flushes buffered data.
func (pw *PageWriter) Flush() error {
	_, err := pw.flush()
	return err
}

// FlushN flushes buffered data and returns the number of written bytes.
func (pw *PageWriter) FlushN() (int, error) {
	return pw.flush()
}

func (pw *PageWriter) flush() (int, error) {
	if pw.bufferedBytes == 0 {
		return 0, nil
	}
	n, err := pw.w.Write(pw.buf[:pw.bufferedBytes])
	pw.pageOffset = (pw.pageOffset + pw.bufferedBytes) % pw.pageBytes
	pw.bufferedBytes = 0
	return n, err
}
