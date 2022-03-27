package com.rodneybeede.software.hashcomparedirectorytreereport;


import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.log4j.AsyncAppender;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Level;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * Output is http://www.iana.org/assignments/media-types/text/tab-separated-values
 * 
 * <p>CSV source headers are "Size-bytes,Hash,File"</p>
 * 
 * @author rbeede
 *
 */
public class Main {
	private static final Logger log = Logger.getLogger(Main.class);
	
	
	private static final String[] SOURCE_HEADERS = {"Size-bytes", "Hash", "File"};  // in order
	
	public static void main(final String[] args) throws IOException {
		if(null == args || 0 == args.length) {
			System.err.println("Incorrect number of arguments");
			System.out.println("Usage:  java -jar " + Main.class.getProtectionDomain().getCodeSource().getLocation().getFile() + " <csv output file> [<csv output file>] [...]");
			System.exit(255);
			return;
		}
		
		
		setupLogging();
		
		log.debug("Current working directory is:  " + System.getProperty("user.dir"));
				
		
		// Parse config options as Canonical paths
		final Path[] csvReportFilePaths = new Path[args.length];
		
		for(int i = 0; i < args.length; i++) {
			csvReportFilePaths[i] = Paths.get(args[i]).toAbsolutePath();  // RealPath doesn't exist yet
			log.info("CSV report file #" + i + " path (real canonical) is " + csvReportFilePaths[i]);
		}


		final HashMap<String,ArrayList<String>> hashesToPathBytes = new HashMap<>();  // HashMap is not thread safe, single thread operations please
		
		for(final Path csvReportFilePath : csvReportFilePaths) {
			log.info("Parsing " + csvReportFilePath);
			
			parseCsvReport(csvReportFilePath, hashesToPathBytes);
			
			log.info("Done parsing " + csvReportFilePath);
		}
		
		// Some info stats
		log.info("Found " + hashesToPathBytes.size() + " parsed hashes");
		
		
		// Generate our reports!
		log.info("Generating report files...");
		generateReports(hashesToPathBytes);
		log.info("Creation of reports complete");
		
		
		// Exit with appropriate status
		log.info("Program has completed");
		

		LogManager.shutdown();;  //Forces log to flush (incase using async or buffered output)
		
		System.exit(0);  // All good
	}
	
	
	private static void parseCsvReport(final Path csvReportFilePath, final HashMap<String, ArrayList<String>> hashesToPathBytes) throws IOException {
		final Reader reader = new FileReader(csvReportFilePath.toFile());
		
		for(final CSVRecord csvRecord : CSVFormat.RFC4180.withHeader(SOURCE_HEADERS).parse(reader)) {
			final String sizeInBytes = csvRecord.get(SOURCE_HEADERS[0]).trim();
			final String hash = csvRecord.get(SOURCE_HEADERS[1]).trim().toLowerCase();  // Standardize
			final String filepathname = csvRecord.get(SOURCE_HEADERS[2]);
			
			
			// Get existing matching hash entry ArrayList of data
			ArrayList<String> pathTabBytesList = hashesToPathBytes.get(hash);  // null means either key -> 'value of null' OR key not found
			
			// Brand new, never seen before, hash ?
			if(null == pathTabBytesList) {
				pathTabBytesList = new ArrayList<String>();
				
				hashesToPathBytes.put(hash, pathTabBytesList);  // Add object reference to our hash to record results
				
				log.debug("Added brand new record for first time hash");
			}  // else we already have the live reference which makes LIVE changes to data
			
			
			// Add new csv record entry with tab separator
			pathTabBytesList.add(filepathname  +  "\t"  + sizeInBytes);
			
			if(log.isTraceEnabled()) {
				log.trace(csvRecord.toString());
			}
		}
		
		reader.close();
	}
	
	
	private static void generateReports(final HashMap<String, ArrayList<String>> hashesToPathBytes) throws IOException {
		// First prepare our 2 report type files so we can write as we go
		final FileWriter orphansWriter = new FileWriter("HASH-REPORT_ORPHANS__" + getFormattedDatestamp(null) + ".tsv");
		final FileWriter duplicatesWriter = new FileWriter("HASH-REPORT_DUPLICATES__" + getFormattedDatestamp(null) + ".tsv");
		
		for(final FileWriter writer : new FileWriter[]{orphansWriter, duplicatesWriter}) {
			writer.write("HASH");
			writer.write("\t");

			writer.write("PATH");
			writer.write("\t");

			writer.write("BYTES");

			writer.write("\n");  // Always this regardless of platform

			writer.flush();
		}
		
		
		for(final Entry<String,ArrayList<String>> entry : hashesToPathBytes.entrySet()) {
			final FileWriter whichWriter;
			
			if(entry.getValue().size() > 1) {
				whichWriter = duplicatesWriter;
			} else {
				whichWriter = orphansWriter;
				log.debug("Recording orphan " + entry.toString());
			}
			
			for(final String pathTabBytes : entry.getValue()) {
				whichWriter.write(entry.getKey());
				whichWriter.write("\t");
				
				whichWriter.write(pathTabBytes);
				
				whichWriter.write("\n");  // Always this regardless of platform
			}
		}
		
		
		orphansWriter.close();
		duplicatesWriter.close();
	}


	private static void setupLogging() {
		final Layout layout = new PatternLayout("%d{yyyy-MM-dd HH:mm:ss,SSS Z}\t%-5p\tThread=%t\t%c\t%m%n");
		
		
		// Use an async logger for speed
		final AsyncAppender asyncAppender = new AsyncAppender();
		asyncAppender.setThreshold(Level.ALL);
		
		Logger.getRootLogger().setLevel(Level.ALL);
		Logger.getRootLogger().addAppender(asyncAppender);
		
		
		// Setup the logger to also log to the console
		final ConsoleAppender consoleAppender = new ConsoleAppender(layout);
		consoleAppender.setEncoding("UTF-8");
		consoleAppender.setThreshold(Level.INFO);
		asyncAppender.addAppender(consoleAppender);
		
		
		// Setup the logger to log into the current working directory
		final File logFile = new File(System.getProperty("user.dir"), getFormattedDatestamp(null) + ".log");
		final FileAppender fileAppender;
		try {
			fileAppender = new FileAppender(layout, logFile.getAbsolutePath());
		} catch (final IOException e) {
			e.printStackTrace();
			log.error(e,e);
			return;
		}
		fileAppender.setEncoding("UTF-8");
		fileAppender.setThreshold(Level.ALL);
		asyncAppender.addAppender(fileAppender);
		
		System.out.println("Logging to " + logFile.getAbsolutePath());
	}
	
	
	private static String getFormattedDatestamp(final Date date) {
		final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss_Z");
		
		if(null == date) {
			return dateFormat.format(new Date());
		} else {
			return dateFormat.format(date);
		}
	}
}
