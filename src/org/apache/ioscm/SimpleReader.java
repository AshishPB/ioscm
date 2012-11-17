package org.apache.ioscm;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.w3c.dom.Element;

public class SimpleReader extends IOStream{
	String dataPath;
	int interval; //milliseconds
	long size; //MB
	int rsize; //bytes

	public SimpleReader(String dataPath, int interval, long size, int rsize, String label) {
		this.dataPath = dataPath;
		this.interval = interval;
		this.size = size;
		this.rsize = rsize;		
		setLabel(label);
	}
	
	public SimpleReader(Element sl) {
		dataPath = getTextValue(sl,"Path");
		interval = getIntValue(sl,"interval");
		size = getIntValue(sl,"size");
		rsize = getIntValue(sl,"rsize");	
		setLabelFromXML(sl);
	}

	
	public void run() {
		LOG.info("SimpleReader\t" + "\t" + dataPath + "\t"
				+ Integer.toString(interval) + "\t" + Long.toString(size) + "\t"
				+ Integer.toString(rsize));
		char[] cbuf = new char[rsize];
		try {
			BufferedReader in = new BufferedReader(new FileReader(dataPath), rsize);
			long cur = 0;
			sync();
			
			while (cur <= size) {
				timerOn();
				in.read(cbuf, 0, rsize);
				timerOff(cur, rsize);
				cur += rsize;
				if (interval > 0)
					synchronized(this){
						wait(interval);
					}
			}
			in.close();
		}
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
}
