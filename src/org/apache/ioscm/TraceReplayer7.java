package org.apache.ioscm;

import java.lang.Integer;
import java.lang.Long;
import java.lang.IllegalStateException;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;
import java.io.FileDescriptor;
import java.io.RandomAccessFile;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.*;
import java.nio.channels.FileChannel;

import java.util.Random;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.EnumSet;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Future;

import org.apache.commons.lang.ArrayUtils;
import org.w3c.dom.Element;


public class TraceReplayer7 extends IOStream {
	private enum SyncMode {
		SYNC_ALL, ASYNC_ALL, DEFAULT;
	}	

	String dataPath;
	String tracePath;
	long period; //seconds
	long max;
	long blockSize = 1;
	int threadPerTrace = 1;
	ExecutorService pool;
	float iscale;
	SyncMode syncMode = SyncMode.DEFAULT;
	
	public class IOReqWrap {
		
		long offset;
		int size;
		String op;
		long submit; 	//submit time
		long s; 		//start time
		long e;			//end time
		
		public IOReqWrap(long offset, int size, String op){
			this.offset = offset;
			this.size = size;
			this.op = op;
		}
		
		public IOReqWrap(long offset, int size, String op, long s){
			this.offset = offset;
			this.size = size;
			this.op = op;
			this.s = s;
		}
		
		public long startAt(){
			return s;
		}
		
		public void startAt(long s){
			this.s = s;
		}
		
		public long endAt(){
			return e;
		}
		
		public void endAt(long e){
			this.e = e;
		}
		
		public long getOffset(){
			return offset;
		}
		
		public int getSize(){
			return size;
		}
		
		public String getOP(){
			return op;
		}
		
	}

	public TraceReplayer7(String dataPath, String tracePath, long period, String label, int blockSize, String syncMode, float iscale, ExecutorService pool) {
		this.dataPath = dataPath;
		this.tracePath = tracePath;
		this.period = period;
		this.blockSize = blockSize;
		this.syncMode = SyncMode.valueOf(syncMode);
		this.iscale = iscale;
		this.pool = pool;
		setLabel(label);
	}
	
	public TraceReplayer7(Element sl) {
		dataPath = getTextValue(sl,"dataPath");
		tracePath = getTextValue(sl,"tracePath");
		period = getLongValue(sl, "period");
		blockSize = getIntValue(sl, "blockSize");
		threadPerTrace = getIntValue(sl,"threadPerTrace");
		syncMode = SyncMode.valueOf(getTextValue(sl, "SyncMode") );
		iscale = getFloatValue(sl,"intervalScaleFactor");
		setLabelFromXML(sl);
	}
	
	public static enum OP {
	    AR, // R(asynchronous read)
	    BR, // r(blocking read)
	    AW, // W(asynchronous write)
	    BSW // w(blocking write and sync) 
    }
	
	public void run() {	
		Path dp=Paths.get(dataPath);
		if (pool == null) {
			pool= new ScheduledThreadPoolExecutor(threadPerTrace);
		}
		
		
		LOG.info("TraceReplayer7\t" + "\t" + dataPath + "\t" + tracePath + "\t"
				+ "\t" + Long.toString(period)); 
			
		
		try {
			AsynchronousFileChannel fc=AsynchronousFileChannel.open(dp, EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE), pool);
			CompletionHandler<Integer, IOReqWrap> handler= new CompletionHandler<Integer, IOReqWrap>(){

				@Override
				public synchronized void completed(Integer result, IOReqWrap req) {
					OPCompleteEvent(req.startAt(), System.nanoTime(), req.getOffset(), req.getSize(), req.getOP());	
				}		

				@Override
				public void failed(Throwable exc, IOReqWrap req) {
    					exc.printStackTrace();
				}
			};

			// long offset;
			// int rsize = 65536; //bytes
			// int interval; //milliseconds
			String btrl;
			String args[];
			
			File trace = new File(tracePath);
			FileReader tr = new FileReader(trace);
			BufferedReader btr  = new BufferedReader(tr, 65536);
			List<Long> offsetList = new ArrayList<Long>();
			List<Integer> rsizeList = new ArrayList<Integer>();
			List<OP> opList = new ArrayList<OP>();
			List<Integer> intervalList = new ArrayList<Integer>();
			
			while ((btrl = btr.readLine()) != null) {
				args = btrl.split("[|]");
				String op = args[2];
				if (syncMode == SyncMode.SYNC_ALL)
					op = op.toLowerCase();
				else if (syncMode == SyncMode.ASYNC_ALL)
					op = op.toUpperCase();
				OP thisOP = null;
				if (op.contentEquals("R")) {
					thisOP = OP.AR;
				}
				else if (op.contentEquals("r")) {
					thisOP = OP.BR;
				}
				else if (op.contentEquals("W") ) {
					thisOP = OP.AW;
				}
				else if (op.contentEquals("w")) {
					thisOP = OP.BSW;
				} else {
					// skip unsupported operation
					continue;
				}
				offsetList.add(Long.parseLong(args[0]) * blockSize);
				rsizeList.add(Integer.parseInt(args[1]));
				opList.add(thisOP);
				intervalList.add(Math.round( Float.parseFloat(args[3]) * 1000 * iscale)); //millisecond
			}
			btr.close();
			
			long[] offsetArray = ArrayUtils.toPrimitive(offsetList.toArray(new Long[0]));
			int[] rsizeArray = ArrayUtils.toPrimitive(rsizeList.toArray(new Integer[0]));
			OP[] opArray = opList.toArray(new OP[0]);
			int[] intervalArray = ArrayUtils.toPrimitive(intervalList.toArray(new Integer[0]));
			
			int opCount = offsetArray.length;
			assert opCount == rsizeArray.length;
			assert opCount == opArray.length;
			assert opCount == intervalArray.length;
			
			RandomAccessFile rf = new RandomAccessFile(new File(dataPath), "rwd");
			FileDescriptor rfd = rf.getFD();
			FileChannel rfc = rf.getChannel();
			max = rf.length();

			List<Future<Integer>> futures = new ArrayList<>();
			
			sync();
			long start = System.nanoTime();
			for (int i=0; i<opCount; i++) {
				long offset = offsetArray[i];
				int rsize = rsizeArray[i];
				if (rsize <= 0 || offset < 0) {
					continue;
				}
				
				ByteBuffer buf = ByteBuffer.allocateDirect(rsize);
				
				timerOn();
				try {
					switch (opArray[i]) {
					case AR: {
						IOReqWrap req = new IOReqWrap(offset, rsize, "R", System.nanoTime());
						fc.read(buf, offset, req, handler);
						break;
					}
					case BR: {
						rfc.position(offset);
						rfc.read(buf); 		//get blocked until the requested number of bytes are read
						timerOff(offset, rsize, "r");
						break;
					}
					case AW: {
						IOReqWrap req = new IOReqWrap(offset, rsize, "W", System.nanoTime());
						fc.write(buf, offset, req, handler);
						break;
					}
					case BSW: {
						rfc.position(offset);
						rfc.write(buf);
						rfc.force(false);
						rfd.sync();
						timerOff(offset, rsize, "w");
						break;
					}
					}
				} catch (IllegalStateException e) {
					System.out.println("Offset is too large: " + offset);
				}
				
				int interval = intervalArray[i];
				if (interval > 0) {
					synchronized(this){
						wait(interval);
					}
				}
			}
			
			for (Future<Integer> future : futures) {
				try {
					future.get();
				} catch (ExecutionException e) {
					System.out.println("Task wasn't executed!");
				}
			}

			rf.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		LOG.info("--TraceReplayer7");
	}
}

