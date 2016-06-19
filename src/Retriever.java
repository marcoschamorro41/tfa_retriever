import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;

import twitter4j.FilterQuery;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterObjectFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.conf.ConfigurationBuilder;


public class Retriever {

	private static final String TWITTER_CONSUMER_KEY = "fcyCZ3IbEIEoQTyga3PYbUzkM";
	private static final String TWITTER_SECRET_KEY = "5GRNcYO7QiLpHgHW22pgzUiOFX61OInizBWxOYWHP6d2RR1KHj";
	private static final String TWITTER_ACCESS_TOKEN = "164936578-keERLE7jTtXMNQwtm9Fv18mZ1fm8lvUj6oVpFsLe";
	private static final String TWITTER_ACCESS_TOKEN_SECRET = "vqwTdozZqVkK7OEfKzscJ28g93HOLGg3to24qnJw1sqbg";

	private TwitterStream twitterStream;
	private String[] keywords;
	FileOutputStream fos;
	ConfigurationBuilder cb;
	
	public Retriever() {
		this.crearCarpetaTMP();
		this.setup();
		
	}
	
	public void crearCarpetaTMP(){
	       File carpeta = new File("c://hadoop");
	       
	       if(!carpeta.exists()){
	           carpeta.mkdir();
	       }
	       
	       
	   }

	StatusListener listener = new StatusListener() {

		// The onStatus method is executed every time a new tweet comes in.
		public void onStatus(Status status) {
			// The EventBuilder is used to build an event using the headers and
			// the raw JSON of a tweet
			System.out.println(status.getUser().getScreenName() + ": "
					+ status.getText());

			System.out.println("timestamp : "
					+ String.valueOf(status.getCreatedAt().getTime()));
			try {
				fos.write(TwitterObjectFactory.getRawJSON(status).getBytes());
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		// This listener will ignore everything except for new tweets
		public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
		}

		public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
		}

		public void onScrubGeo(long userId, long upToStatusId) {
		}

		public void onException(Exception ex) {
		}

		public void onStallWarning(StallWarning warning) {
		}
	};

	public void setup() {
		cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true).setOAuthConsumerKey(TWITTER_CONSUMER_KEY)
				.setOAuthConsumerSecret(TWITTER_SECRET_KEY)
				.setOAuthAccessToken(TWITTER_ACCESS_TOKEN)
				.setOAuthAccessTokenSecret(TWITTER_ACCESS_TOKEN_SECRET);
		cb.setJSONStoreEnabled(true);
		cb.setIncludeEntitiesEnabled(true);
		twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
	}
	
	public void startTwitter(String queries) {
		 
		try {
			fos = new FileOutputStream(new File("c://hadoop//twitterstream.json"));
		} catch (IOException e) {
			e.printStackTrace();
		}
 
	    keywords = queries.split(",");
		for (int i = 0; i < keywords.length; i++) {
			keywords[i] = keywords[i].trim();
		}
 
		// Set up the stream's listener (defined above),
		twitterStream.addListener(listener);
 
		System.out.println("Starting down Twitter sample stream...");
 
		// Set up a filter to pull out industry-relevant tweets
		FilterQuery query = new FilterQuery().track(keywords);
		twitterStream.filter(query);
 
	}
	
	public void stopTwitter() {
		 
		System.out.println("Shutting down Twitter sample stream...");
		twitterStream.shutdown();
 
		try {
			fos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	

	public void getTwitters(String pquery) {
		ConfigurationBuilder cb = new ConfigurationBuilder();
		cb.setDebugEnabled(true).setOAuthConsumerKey(TWITTER_CONSUMER_KEY)
				.setOAuthConsumerSecret(TWITTER_SECRET_KEY)
				.setOAuthAccessToken(TWITTER_ACCESS_TOKEN)
				.setOAuthAccessTokenSecret(TWITTER_ACCESS_TOKEN_SECRET);
		TwitterFactory tf = new TwitterFactory(cb.build());
		Twitter twitter = tf.getInstance();
		try {
			Query query = new Query(pquery);
			QueryResult result;
			do {
				result = twitter.search(query);
				List<Status> tweets = result.getTweets();
				for (Status tweet : tweets) {
					System.out.println("@" + tweet.getUser().getScreenName()
							+ " - " + tweet.getText());
					System.out.println(TwitterObjectFactory.getRawJSON(tweet));
				}
			} while ((query = result.nextQuery()) != null);
			System.exit(0);
		} catch (TwitterException te) {
			te.printStackTrace();
			System.out.println("Failed to search tweets: " + te.getMessage());
			System.exit(-1);
		}
	}

	public static String getHTML(String urlToRead) throws Exception {
		StringBuilder result = new StringBuilder();
		URL url = new URL(urlToRead);
		HttpURLConnection conn = (HttpURLConnection) url.openConnection();
		conn.setRequestMethod("GET");
		conn.setRequestProperty(
				"Authorization",
				"OAuth oauth_consumer_key=\"fcyCZ3IbEIEoQTyga3PYbUzkM\", oauth_nonce=\"2023d615a46134d870b495c3ae4e8072\", oauth_signature=\"xkggsQ%2B41ks8eBAur9PKLZfvXpg%3D\", oauth_signature_method=\"HMAC-SHA1\", oauth_timestamp=\"1465863417\", oauth_token=\"164936578-keERLE7jTtXMNQwtm9Fv18mZ1fm8lvUj6oVpFsLe\", oauth_version=\"1.0\"");
		BufferedReader rd = new BufferedReader(new InputStreamReader(
				conn.getInputStream()));
		String line;
		while ((line = rd.readLine()) != null) {
			result.append(line);
		}
		rd.close();
		return result.toString();
	}
}