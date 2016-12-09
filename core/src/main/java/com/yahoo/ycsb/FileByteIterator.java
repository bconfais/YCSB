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
	private static int offset = 0;
	private static long range = 1000000000L;
	static {
		try {
			file = new FileReader("/tmp/dummy");
			Random r = new Random();
			file.skip((long)(r.nextDouble()*range));
		} catch(java.io.FileNotFoundException e) {
			System.out.println("dummy file not found");
			file = null;
		} catch(IOException e) {
			System.out.println("read dummy file failed");
			file = null;
		}
	}

	public FileByteIterator() {

	}

	@Override
	synchronized public boolean hasNext() {
		try {
			return file.ready();
		} catch (IOException e) {
			return false;
		}
	}

	@Override
	synchronized public byte nextByte() {
		byte ret;
		if ( file == null ) {
			return (byte) 0;
		}
		try {
			ret = (byte) file.read();
		} catch (IOException e) {
			System.out.println("read dummy file failed, use the random generator instead");
			return (byte) 0;
		}
		return ret;
	}

	@Override
	synchronized public long bytesLeft() {
		try {
			if ( file.ready() ) {
				return 1;
			} else {
				return 0;
			}
		} catch (IOException e) {
			return 0;
		}
	}

}
