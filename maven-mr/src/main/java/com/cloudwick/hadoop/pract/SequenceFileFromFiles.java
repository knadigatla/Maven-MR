package com.cloudwick.hadoop.pract;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.zip.GZIPInputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.tools.bzip2.CBZip2InputStream;
import org.apache.tools.tar.TarEntry;
import org.apache.tools.tar.TarInputStream;

import com.cloudwick.mr.seq.LocalSetup;
import com.cloudwick.mr.seq.TarToSeqFile;

public class SequenceFileFromFiles {

    private File inputFile;
    private File outputFile;
    private LocalSetup setup;

    

    /** Sets the input tar file. */
    public void setInput(File inputFile) {
        this.inputFile = inputFile;
    }

    /** Sets the output SequenceFile. */
    public void setOutput(File outputFile) {
        this.outputFile = outputFile;
    }

    /** Performs the conversion. */
    public void execute() throws Exception {
        TarInputStream input = null;
        SequenceFile.Writer output = null;
        try {
            input = openInputFile();
            output = openOutputFile();
            TarEntry entry;
            while ((entry = input.getNextEntry()) != null) {
                if (entry.isDirectory()) { continue; }
                String filename = entry.getName();
                byte[] data = SequenceFileFromFiles.getBytes(input, entry.getSize());
                
                Text key = new Text(filename);
                BytesWritable value = new BytesWritable(data);
                output.append(key, value);
            }
        } finally {
            if (input != null) { input.close(); }
            if (output != null) { output.close(); }
        }
    }

    private TarInputStream openInputFile() throws Exception {
        InputStream fileStream = new FileInputStream(inputFile);
        String name = inputFile.getName();
        InputStream theStream = null;
        if (name.endsWith(".tar.gz") || name.endsWith(".tgz")) {
            theStream = new GZIPInputStream(fileStream);
        } else if (name.endsWith(".tar.bz2") || name.endsWith(".tbz2")) {
            /* Skip the "BZ" header added by bzip2. */
            fileStream.skip(2);
            theStream = new CBZip2InputStream(fileStream);
        } else {
            /* Assume uncompressed tar file. */
            theStream = fileStream;
        }
        return new TarInputStream(theStream);
    }

    private SequenceFile.Writer openOutputFile() throws Exception {
        Path outputPath = new Path(outputFile.getAbsolutePath());
        return SequenceFile.createWriter(setup.getLocalFileSystem(), setup.getConf(),
                                         outputPath,
                                         Text.class, BytesWritable.class,
                                         SequenceFile.CompressionType.BLOCK);
    }

    /** Reads all bytes from the current entry in the tar file and
     * returns them as a byte array.
     *
     * @see http://www.exampledepot.com/egs/java.io/File2ByteArray.html
     */
    private static byte[] getBytes(TarInputStream input, long size) throws Exception {
        if (size > Integer.MAX_VALUE) {
            throw new Exception("A file in the tar archive is too large.");
        }
        int length = (int)size;
        byte[] bytes = new byte[length];

        int offset = 0;
        int numRead = 0;

        while (offset < bytes.length &&
               (numRead = input.read(bytes, offset, bytes.length - offset)) >= 0) {
            offset += numRead;
        }

        if (offset < bytes.length) {
            throw new IOException("A file in the tar archive could not be completely read.");
        }

        return bytes;
    }

    /** Runs the converter at the command line. */
    public static void main(String[] args) {
        if (args.length != 2) {
            exitWithHelp();
        }

        SequenceFile.Writer writer = null;
        FSDataInputStream in = null;

        try {
//        String uri = args[1];
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(args[1]), conf);
        Path inPath = new Path(args[0]);
        Path outPath = new Path(args[1]);

        FileStatus[] status = fs.listStatus(inPath);
        Path[] listedPaths = FileUtil.stat2Paths(status);
        
        
//        Text key = new Text();
//        BytesWritable value = new BytesWritable();
        
          writer = SequenceFile.createWriter(fs, conf, outPath,
              Text.class, BytesWritable.class);
          
          for (int i = 0; i < listedPaths.length; i++) {
//            key.set(listedPaths[i].getName());
            
            in = fs.open(listedPaths[i]);
            
            byte[] data = new byte[(int)status[i].getLen()];
            
            in.read(data, 0, (int)status[i].getLen());
            
            Text key = new Text(listedPaths[i].getName());
            BytesWritable value = new BytesWritable(data);
            
            writer.append(key, value);

            System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
//            writer.append(key, value);
            
            in.close();
          }
          
//          SequenceFileFromFiles me = new SequenceFileFromFiles();
//          me.setInput(new File(args[0]));
//          me.setOutput(new File(args[1]));
//          me.execute();
          
        } 
        catch (Exception e) {
            e.printStackTrace();
            exitWithHelp();
        }
        finally {
          IOUtils.closeStream(writer);
          IOUtils.closeStream(in);
        }
      

       
    }

    public static void exitWithHelp() {
        System.err.println("Something went wrong exiting program..!!");
        System.exit(1);
    }
}
