/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */
package com.yahoo.ycsb;
import java.io.FileReader;
import java.io.IOException;
import java.util.Random;

public class FileByteIterator extends ByteIterator {
	private static FileReader file;
	private static int max_skip = 100000000;
	private static final String filename = "/tmp/dummy";
	private long len;
	private long off;
	private int bufOff;
	private byte[] buf;

	static {
		init_file();
	}

	private static void init_file() {
		try {
			file = new FileReader(filename);
			file.skip(skip());
		} catch(java.io.FileNotFoundException e) {
			System.out.println("dummy file not found");
			file = null;
		} catch(IOException e) {
			System.out.println("read dummy file failed");
			file = null;
		}
	}

	private static long skip() {
		Random r = new Random();
		return (long) r.nextInt(max_skip);
	}


	private void fillBytesImpl(byte[] buffer, int base) {
		int bytes;
		try {
			bytes = file.read();
		} catch (IOException e) {
			System.out.println("read dummy file failed, use the random generator instead");
			init_file();
			bytes = 0;
		}
		switch(buffer.length - base) {
			default:
				buffer[base+5] = (byte)(((bytes >> 25) & 95) + ' ');
			case 5:
				buffer[base+4] = (byte)(((bytes >> 20) & 63) + ' ');
			case 4:
				buffer[base+3] = (byte)(((bytes >> 15) & 31) + ' ');
			case 3:
				buffer[base+2] = (byte)(((bytes >> 10) & 95) + ' ');
			case 2:
				buffer[base+1] = (byte)(((bytes >> 5) & 63) + ' ');
			case 1:
				buffer[base+0] = (byte)(((bytes) & 31) + ' ');
			case 0:
				break;
		}
	}

	private void fillBytes() {
		fillBytesImpl(buf, 0);
		bufOff = 0;
		off += buf.length;
	}

	public FileByteIterator(long len) {
		this.len = len;
		this.buf = new byte[6];
		this.bufOff = buf.length;
		fillBytes();
		this.off = 0;
	}

	@Override
	public boolean hasNext() {
		return (off + bufOff) < len;
	}

	public byte nextByte() {
		fillBytes();
		bufOff++;
		return buf[bufOff-1];
	}

	@Override
	public int nextBuf(byte[] buffer, int bufferOffset) {
		int ret;
		if(len - off < buffer.length - bufferOffset) {
			ret = (int)(len - off);
		} else {
			ret = buffer.length - bufferOffset;
		}
		int i;
		for(i = 0; i < ret; i+=6) {
			fillBytesImpl(buffer, i + bufferOffset);
		}
		off+=ret;
		return ret + bufferOffset;
	}

	@Override
	public long bytesLeft() {
		return len - off - bufOff;
	}

}
