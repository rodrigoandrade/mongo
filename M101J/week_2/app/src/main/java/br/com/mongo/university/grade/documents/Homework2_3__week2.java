package br.com.mongo.university.grade.documents;

import static com.mongodb.client.model.Filters.eq;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;

/**
 * 
 * @author rodrigo
 *
 * Homework 2.3, week 2 
 */
public class Homework2_3__week2 {
	
	private static MongoClient client;

	public static void main(String[] args) throws Exception {
		
		client = new MongoClient();
		MongoDatabase db = client.getDatabase("students");
		MongoCollection<Document> coll = db.getCollection("grades");

		Bson sort = Sorts.ascending("student_id", "score");
		Bson filter = eq("type", "homework");

		List<Document> documents = coll.find(filter).sort(sort).into(new ArrayList<Document>());
		
		int studentId = Integer.MIN_VALUE;
		List<Document> listDocumentForRemove = new ArrayList<>();
		for (Document document : documents) {
			if (studentId != (Integer) document.get("student_id")) {
				listDocumentForRemove.add(document);
			}
			studentId = (Integer) document.get("student_id");
		}
		
		if (listDocumentForRemove.size() != 200) {
			throw new Exception("It had to be 200 to remove!");
		}
		
		final List<ObjectId> idsForRemoved = new ArrayList<>();
		for (Document document : listDocumentForRemove) {
			idsForRemoved.add(document.getObjectId("_id"));
		}
		
		Bson filterToRemove = Filters.in("_id", idsForRemoved);
        coll.deleteMany(filterToRemove).getDeletedCount();
        
	}

}
