package edu.umass.ciir.galagoexport;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import org.lemurproject.galago.core.index.corpus.CorpusReader;
import org.lemurproject.galago.core.index.corpus.DocumentReader;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.StreamCreator;

public class Main {
  
  private static PrintWriter outWriter(String outputDir, String outputPrefix, int outputIndex) throws IOException {
    String filePath = new File(outputDir, outputPrefix+outputIndex+".trectext.gz").getAbsolutePath();
    DataOutputStream dos = StreamCreator.openOutputStream(filePath);
    return new PrintWriter(dos);
  }

  public static void main(String[] args) throws IOException, Exception {
    Parameters argp = new Parameters(args);
    
    String inputIndex = argp.getString("inputIndex");
    String outputDir = argp.getString("outputDir");
    String outputPrefix = argp.get("outputPrefix", "fromIndex");
    long documentsPerSplit = argp.get("numDocs", 50000);
    
    File corpus = new File(inputIndex, "corpus");
    if(!corpus.exists()) {
      throw new RuntimeException("No corpus file or directory found for index!");
    }
    
    DocumentReader dr = new CorpusReader(corpus.getAbsolutePath());
    DocumentReader.DocumentIterator iter = (DocumentReader.DocumentIterator) dr.getIterator();
    
    int outputIndex = 0;
    long numDocuments = 0;
    
    Parameters docP = new Parameters();
    docP.set("metadata", false);
    docP.set("terms", false);
    docP.set("text", true);
    
    PrintWriter out = outWriter(outputDir, outputPrefix, outputIndex);
    while(!iter.isDone()) {
      Document doc = iter.getDocument(argp);
      iter.nextKey();
      if(doc == null) continue;
      
      // output trec to output stream
      out.println("<DOC>");
      
      out.print("<DOCNO>");
      out.print(doc.name);
      out.println("</DOCNO>");
      
      out.println("<TEXT>");
      out.println(doc.text);
      out.println("</TEXT>");
      
      out.println("</DOC>");
      
      numDocuments += 1;
      if(numDocuments == documentsPerSplit) {
        numDocuments = 0;
        out.close();
        out = outWriter(outputDir, outputPrefix, ++outputIndex);
      }
    }
    out.close();
  }
}
