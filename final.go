package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
	"gopkg.in/yaml.v2"
)

type Retweeters struct {
	gorm.Model
	User      string
	Retweet   uint
	TweetUser int
}

func getMostActiveRetweeter(db *gorm.DB, lengthOfTweets int, userHandle string, queryIdentifier int) string {
	var result Retweeters
	db.Raw("select * from retweeters where tweet_user=? order by retweet desc limit 1", queryIdentifier).Scan(&result)
	mostActiveRetweeter := result.User
	fmt.Printf("The most active retweeter of the last %+v tweets of %+v is %+v", lengthOfTweets, userHandle, mostActiveRetweeter)
	return mostActiveRetweeter
}
func maxLengthOfTweets(lenOftweets int) int {
	if lenOftweets < 100 {
		return lenOftweets
	}

	return 100
}

func worker(id int, twitID int64, queryIdentifier int, client *twitter.Client, db *gorm.DB, wg *sync.WaitGroup) {
	defer wg.Done()
	twits, _, err := client.Statuses.Retweets(twitID, &twitter.StatusRetweetsParams{Count: 100})
	if err != nil {
		log.Fatal("Failed to fetch the retweets ", err)
	}
	for _, twit := range twits {
		var retwet Retweeters
		retwitName := twit.User.Name
		db.Raw("select * from retweeters where user=? and tweet_user=?", retwitName, queryIdentifier).Scan(&retwet)
		if retwet.User != "" {
			db.Exec("UPDATE Retweeters SET retweet = retweet+1 WHERE user = ? and tweet_user=?", retwitName, queryIdentifier)
		} else {
			db.Create(&Retweeters{User: retwitName, Retweet: 1, TweetUser: queryIdentifier})
		}
	}
}
func getTweets(c *gin.Context, client *twitter.Client, userHandle string) []twitter.Tweet {
	falsevalue := false
	truevalue := true
	UserTimelineParams := &twitter.UserTimelineParams{
		ScreenName:      userHandle,
		Count:           400,
		IncludeRetweets: &falsevalue,
		ExcludeReplies:  &truevalue,
	}
	tweets, _, err := client.Timelines.UserTimeline(UserTimelineParams)
	if err != nil {
		log.Fatal("Failed to fetch the tweets ", err)
	}
	return tweets
}
func getClient(c *gin.Context, userHandle string) (client *twitter.Client) {
	var keys struct {
		Key    string `json:"consumer_key"`
		Secret string `json:"consumer_secret"`
	}
	f, err := os.Open(".keys.json")
	if err != nil {
		log.Fatal("Failed to open the credentials file ", err)
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	dec.Decode(&keys)

	if keys.Key == "" || keys.Secret == "" {
		log.Fatal("Application Access Token required")
	}

	if userHandle == "" {
		log.Fatal("Userhandle required")
	}
	// ozzo-validation
	// oauth2 configures a client that uses app credentials to keep a fresh token
	config := &clientcredentials.Config{
		ClientID:     keys.Key,
		ClientSecret: keys.Secret,
		TokenURL:     "https://api.twitter.com/oauth2/token",
	}
	// http.Client will automatically authorize Requests
	httpClient := config.Client(oauth2.NoContext)

	// Twitter client
	client = twitter.NewClient(httpClient)
	return client
}

func getDatabase() (db *gorm.DB, queryIdentifier int) {
	db, err := gorm.Open("mysql", "root:root@tcp(127.0.0.1:3306)/twitter?charset=utf8mb4&parseTime=True")
	if err != nil {
		log.Fatal("Failed to connect to database ", err)
	}
	db.Debug().AutoMigrate(&Retweeters{})
	db.Exec("ALTER DATABASE twitter CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci;")
	var result Retweeters
	db.Raw("select * from retweeters order by tweet_user desc limit 1").Scan(&result)
	return db, result.TweetUser + 1
}

func maxRetweeter(c *gin.Context) {
	db, queryIdentifier := getDatabase()
	defer db.Close()
	fmt.Println(queryIdentifier)
	userHandle := c.Param("userHandle")
	client := getClient(c, userHandle)
	tweets := getTweets(c, client, userHandle)

	fmt.Printf("Length of user tweets %d\n", len(tweets))

	var wg sync.WaitGroup
	lengthOfTweets := maxLengthOfTweets(len(tweets))
	for i := 0; i < lengthOfTweets; i++ {
		wg.Add(1)
		twitID := tweets[i].ID
		go worker(i, twitID, queryIdentifier, client, db, &wg)
	}

	wg.Wait()
	mostActiveRetweeter := getMostActiveRetweeter(db, lengthOfTweets, userHandle, queryIdentifier)
	c.JSON(200, gin.H{
		"maxRetweeter": mostActiveRetweeter,
	})
}
func YAMLHandler(yamlBytes []byte, fallback http.Handler) (http.HandlerFunc, error) {
	pathUrls, err := parseYaml(yamlBytes)
	if err != nil {
		return nil, err
	}
	pathsToUrls := buildMap(pathUrls)
	return MapHandler(pathsToUrls, fallback), nil
}

func buildMap(pathUrls []pathURL) map[string]string {
	pathsToUrls := make(map[string]string)
	for _, pu := range pathUrls {
		pathsToUrls[pu.Path] = pu.URL
	}
	return pathsToUrls
}

func parseYaml(data []byte) ([]pathURL, error) {
	var pathUrls []pathURL
	err := yaml.Unmarshal(data, &pathUrls)
	if err != nil {
		return nil, err
	}
	return pathUrls, nil
}

func latestTweet(c *gin.Context) {
	db, queryIdentifier := getDatabase()
	defer db.Close()
	fmt.Println(queryIdentifier)
	userHandle := c.Param("userHandle")
	client := getClient(c, userHandle)
	tweets := getTweets(c, client, userHandle)

	fmt.Printf("Length of user tweets %d\n", len(tweets))

	c.JSON(200, gin.H{
		"latestTweets": tweets,
	})
}
unsigned long modPow(unsigned long x, int y) {
	unsigned long tot = 1, p = x;
	for (; y; y >>= 1) {
		if (y & 1)
			tot = (tot * p) % mod97;
		p = (p * p) % mod97;
	}
	return tot;
}
class Fancy {
public:
	unsigned long seq[100001];
	unsigned int length = 0;
	unsigned long increment = 0;
	unsigned long mult = 1;
	Fancy() {
		ios_base::sync_with_stdio(false);
		cin.tie(NULL);
	}
	void append(int val) {
		seq[length++] = (((mod97 + val - increment)%mod97) * modPow(mult, mod97-2))%mod97;
	}
	void addAll(int inc) {
		increment = (increment+ inc%mod97)%mod97;
	}
	void multAll(int m) {
		mult = (mult* m%mod97)%mod97;
		increment = (increment* m%mod97)%mod97;
	}
	int getIndex(int idx) {
		if (idx >= length){
			return -1;
		}else{
			return ((seq[idx] * mult)%mod97+increment)%mod97;
		}
	}
};
func main() {
	router := gin.Default()
	router.GET("/twitter/retweets/:userHandle/max", maxRetweeter)
	router.GET("/twitter/tweet/:userHandle/latest", latestTweet)
	router.Run(":8080")
}
