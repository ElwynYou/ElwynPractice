package nio;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Scanner;

/**
 * @Name nio.TransformFile
 * @Description
 * @Author Elwyn
 * @Version 2017/7/25
 * @Copyright 上海云辰信息科技有限公司
 **/
public class TransformFile {
	public static void main(String[] args) throws IOException {
		new TransformFile().cp1();
	}

	public void cp1() throws IOException {
		long start = System.currentTimeMillis() / 1000;
		FileChannel sourceCh = FileChannel.open(Paths.get("E:\\rhel-server-6.5-x86_64-dvd.iso"),
				StandardOpenOption.READ);

		FileChannel destCh = FileChannel.open(Paths.get("E:\\rhel-server-6.5-x86_64-dvd.iso"),
				StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
		MappedByteBuffer inbb = sourceCh.map(FileChannel.MapMode.READ_ONLY, 0, 1024 * 14 * 1024);
		MappedByteBuffer outbb = sourceCh.map(FileChannel.MapMode.READ_WRITE, 0, 1024 * 14 * 1024);
		byte [] dst =new byte[inbb.limit()];
			inbb.get(dst);
			outbb.put(dst);
		sourceCh.close();
		destCh.close();
		long end = System.currentTimeMillis() / 1000;
		System.out.println(end - start);
	}

	public void cp2() throws IOException {
		long start = System.currentTimeMillis() / 1000;
		FileInputStream fis = new FileInputStream("E:\\rhel-server-6.5-x86_64-dvd.iso");
		FileOutputStream fos = new FileOutputStream("E:\\rhel-server-6.5-x86_64-dvd_copy.iso");
		FileChannel sourceCh = fis.getChannel();
		FileChannel destCh = fos.getChannel();
		sourceCh.transferTo(0, destCh.size(), destCh);
		sourceCh.close();
		destCh.close();
		long end = System.currentTimeMillis() / 1000;
		System.out.println(end - start);
	}

	public void cp3() throws IOException {
		FileInputStream inputStream = new FileInputStream("E:\\rhel-server-6.5-x86_64-dvd.iso");
		FileOutputStream outputStream = new FileOutputStream("E:\\rhel-server-6.5-x86_64-dvd_copy.iso");

		FileChannel iChannel = inputStream.getChannel();
		FileChannel oChannel = outputStream.getChannel();

		ByteBuffer buffer = ByteBuffer.allocate(1024 * 4 * 1024);
		long l1 = System.currentTimeMillis();
		while (true) {
			buffer.clear();//pos=0,limit=capcity，作用是让ichannel从pos开始放数据
			int r = iChannel.read(buffer);
			if (r == -1)//到达文件末尾
				break;
			//buffer.limit(buffer.position());//
			//buffer.position(0);//这两步相当于 buffer.flip();
			buffer.flip();
			oChannel.write(buffer);//它们的作用是让ochanel写入pos - limit之间的数据

		}
		inputStream.close();
		outputStream.close();
		System.out.println("complete:" + (System.currentTimeMillis() - l1));
	}


	public void cp4() throws IOException {
		ByteBuffer byteBuf = ByteBuffer.allocate(1024 * 14 * 1024);
		byte[] bbb = new byte[14 * 1024 * 1024];
		FileInputStream fis = new FileInputStream("E:\\rhel-server-6.5-x86_64-dvd.iso");
		FileOutputStream fos = new FileOutputStream("E:\\rhel-server-6.5-x86_64-dvd_copy.iso");
		FileChannel fc = fis.getChannel();
		long timeStar = System.currentTimeMillis();// 得到当前的时间
		//fc.read(byteBuf);// 1 读取
		MappedByteBuffer mbb = fc.map(FileChannel.MapMode.READ_ONLY, 0, 1024 * 14 * 1024);
		System.out.println(fc.size() / 1024);
		long timeEnd = System.currentTimeMillis();// 得到当前的时间
		System.out.println("Read time :" + (timeEnd - timeStar) + "ms");
		timeStar = System.currentTimeMillis();
		//fos.write(bbb);//2.写入

		//mbb.flip();
		fc.write(mbb);
		timeEnd = System.currentTimeMillis();
		System.out.println("Write time :" + (timeEnd - timeStar) + "ms");
		fos.flush();
		fc.close();
		fis.close();
	}


	public void cp5() {
		File file = new File("D://data.txt");
		long len = file.length();
		byte[] ds = new byte[(int) len];

		try {
			MappedByteBuffer mappedByteBuffer = new RandomAccessFile(file, "r")
					.getChannel()
					.map(FileChannel.MapMode.READ_ONLY, 0, len);
			for (int offset = 0; offset < len; offset++) {
				byte b = mappedByteBuffer.get();
				ds[offset] = b;
			}

			Scanner scan = new Scanner(new ByteArrayInputStream(ds)).useDelimiter(" ");
			while (scan.hasNext()) {
				System.out.print(scan.next() + " ");
			}

		} catch (IOException e) {
		}
	}


}

