package tn.insat.gl4;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.bson.Document;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.UUID;


public class TotalCallsType {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Fire Calls");
        job.setJarByClass(TotalCallsType.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(TotCallsReducer.class);
        job.setReducerClass(TotCallsReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        Path outputPath = new Path(args[1]);
        FileOutputFormat.setOutputPath(job, outputPath);
         if (job.waitForCompletion(true)) {
            // put the output in mongoDB :
            // Connect to the MongoDB server
            MongoClient mongoClient = new MongoClient("localhost", 27017);

            // Get a handle to the database
            MongoDatabase database = mongoClient.getDatabase("fire-calls");

            // Get a handle to the "users" collection
            MongoCollection<Document> collection = database.getCollection("fire-calls-batch");

            // read the output file :
            FileSystem fs = FileSystem.get(conf);
            // Open the output file for reading
             Path outputPath1 = new Path(args[1]+"/part-r-00000");
            BufferedReader reader = new BufferedReader(new InputStreamReader(fs.open(outputPath1)));
            String line;
            while ((line = reader.readLine()) != null) {
                String[] values = line.split("\t");
                // Create a new document
                Document document = new Document();
                UUID uuid = UUID.randomUUID();
                document.append("_id",uuid);
                document.append("type",values[0]);
                document.append("count",values[1]);
                // Insert the document into the collection
                collection.insertOne(document);
            }
            reader.close();

            // Close the connection to the MongoDB server
            mongoClient.close();
        }

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
