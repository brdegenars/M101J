package weekThree;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Updates.*;

/**
 * Created by Bradley on 1/30/2017.
 * weekThree
 */
public class StudentController {

    private static final Logger LOG = LoggerFactory.getLogger ( StudentController.class );

    private static final String STUDENT_SCORES = "scores";
    private static final String SCORE_TYPE = "type";
    private static final String SCORE_VALUE = "score";
    private static final String HOMEWORK_SCORE_TYPE = "homework";


    public static void main ( String[] args ) {
        MongoCollection<Document> studentGrades = initializeMongoCollection ( "school", "students" );

        logCollectionCount ( studentGrades );

        removeLowestHomeworkGradeFromEachStudent ( studentGrades );

        logCollectionCount ( studentGrades );
    }

    private static void logCollectionCount ( MongoCollection<Document> studentGrades ) {
        System.out.println ( "Number of elements: " + studentGrades.count () );
    }

    @SuppressWarnings( "SameParameterValue" )
    private static MongoCollection<Document> initializeMongoCollection ( String database, String collection ) {
        MongoClient client = new MongoClient ();
        MongoDatabase studentsDatabase = client.getDatabase ( database );
        return studentsDatabase.getCollection ( collection );
    }

    @SuppressWarnings( "unchecked" )
    private static void removeLowestHomeworkGradeFromEachStudent ( MongoCollection<Document> studentGrades ) {
        for ( Document student : studentGrades.find () ) {
            List<Document> scores = (List<Document>) student.get ( STUDENT_SCORES );
            Document lowestHomeworkScoreToRemove = findLowestHomeworkScoreToRemove ( scores );
            scores.remove ( lowestHomeworkScoreToRemove );
            student.put ( STUDENT_SCORES, scores );
            studentGrades.updateOne ( eq ( "_id", student.getInteger ( "_id" ) ), set ( STUDENT_SCORES, scores ) );
        }
    }

    private static Document findLowestHomeworkScoreToRemove ( List<Document> scores ) {
        double lowestHomeworkScore = Double.MAX_VALUE;
        Document lowestHomeWorkScoreDocument = null;
        for ( Document score : scores ) {
            String scoreType = score.getString ( SCORE_TYPE );
            if ( HOMEWORK_SCORE_TYPE.equals ( scoreType ) ) {
                double scoreValue = score.getDouble ( SCORE_VALUE );
                if ( scoreValue < lowestHomeworkScore ) {
                    lowestHomeWorkScoreDocument = score;
                }
            }
        }
        return lowestHomeWorkScoreDocument;
    }
}
