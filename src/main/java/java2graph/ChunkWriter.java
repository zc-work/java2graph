package java2graph;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class ChunkWriter<T> {
    private String pathPrefix;
    private int currentChunkIdx = 0;
    private int maxChunkSize;
    private List<T> unwrittenElements = new ArrayList<>();

    // by zhou
    private FileOutputStream output;
    private Gson gson = new GsonBuilder().create();
    private OutputStreamWriter writer;

    public ChunkWriter(String pathPrefix, int maxChunkSize) throws IOException {
        this.pathPrefix = pathPrefix;
        this.maxChunkSize = maxChunkSize;
        // by zhou
        this.output = new FileOutputStream(pathPrefix + '.' + currentChunkIdx + ".json.gz");
        this.writer = new OutputStreamWriter(new GZIPOutputStream(output), "UTF-8");
    }

    public void add(T element) {
        unwrittenElements.add(element);
        if (unwrittenElements.size() >= maxChunkSize) {            
            try {
				writeChunk();
			} catch (IOException e) {
				throw new Error("Cannot write to output chunk file: " + e);
			}
        }
    }

    // by zhou
    public void write(T element) throws IOException {
        try {
            writer.write(gson.toJson(element));
            writer.write("\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void writeChunk() throws IOException {
//        FileOutputStream output = new FileOutputStream(pathPrefix + '.' + currentChunkIdx + ".json.gz");
//        Gson gson = new GsonBuilder().create();
        try {
            Writer writer = new OutputStreamWriter(new GZIPOutputStream(output), "UTF-8");
            writer.write(gson.toJson(unwrittenElements));
            writer.close();
        } finally {
            output.close();
        }

        currentChunkIdx++;
        unwrittenElements.clear();
    }

    public void close() {
        try {
            writeChunk();
        } catch (IOException e) {
            throw new Error("Cannot write to output chunk file: " + e);
        }
    }

    // by zhou
    public void outputclose() {
        try {
            writer.close();
            output.close();
        } catch (IOException e) {
            throw new Error("Cannot write to output chunk file: " + e);
        }
    }

}