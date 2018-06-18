package br.com.mongouniversity;

import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

/**
 * 
 * @author rodrigo
 *
 * Homework 3.1, week 3 
 */
public class Homework3_1 {
	
	private static MongoClient client;

	@SuppressWarnings("unchecked")
	public static void main(String[] args) {
		client = new MongoClient();
		MongoDatabase db = client.getDatabase("school");
		MongoCollection<Document> collection = db.getCollection("students");
		
		MongoCursor<Document> cursor = collection.find().iterator();
		while (cursor.hasNext()) {
			
			Document student = cursor.next();
			List<Document> scores = (List<Document>) student.get("scores");
			
			Document documentWithMinScore = new Document();
			double minScore = Double.MAX_VALUE;
			for (Document score : scores) {

				String type = score.getString("type");
				if ("homework".equals(type)) {
					if (score.getDouble("score") < minScore) {
						
						documentWithMinScore = score;
						minScore = score.getDouble("score");
					}
				}
				
			}
			
			scores.remove(documentWithMinScore);
			
			collection.updateOne(Filters.eq("_id", student.get("_id")), new Document("$set", new Document("scores", scores)));
		}
		
		cursor.close();
	}
	
}
