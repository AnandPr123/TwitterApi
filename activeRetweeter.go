package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/dghubble/go-twitter/twitter"
	"github.com/gin-gonic/gin"
	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/mysql"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

type flagstweet struct {
	ConsumerKey    string `header:"ConsumerKey"`
	ConsumerSecret string `header:"ConsumerSecret"`
}
type Retweeters struct {
	//gorm.Model
	User    string
	Retweet uint
}

func getDatabase() (db *gorm.DB) {
	db, err := gorm.Open("mysql", "root:root@tcp(127.0.0.1:3306)/twitter?charset=utf8mb4&parseTime=True")
	if err != nil {
		panic("failed to connect database")
	}
	db.Exec("drop table finaltable;")
	db.Debug().DropTableIfExists(&Retweeters{})
	db.Debug().AutoMigrate(&Retweeters{})
	return db
}
func getClient(c *gin.Context) (client *twitter.Client) {
	var flags flagstweet

	if err := c.ShouldBindHeader(&flags); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	userHandle := c.Param("userHandle")
	if flags.ConsumerKey == "" || flags.ConsumerSecret == "" || userHandle == "" {
		log.Fatal("Application Access Token and userhandle required")
	}

	// oauth2 configures a client that uses app credentials to keep a fresh token
	config := &clientcredentials.Config{
		ClientID:     flags.ConsumerKey,
		ClientSecret: flags.ConsumerSecret,
		TokenURL:     "https://api.twitter.com/oauth2/token",
	}
	// http.Client will automatically authorize Requests
	httpClient := config.Client(oauth2.NoContext)

	// Twitter client
	client = twitter.NewClient(httpClient)
	return client
}
func getTweets(c *gin.Context, client *twitter.Client) []twitter.Tweet {
	userHandle := c.Param("userHandle")
	falsevalue := false
	truevalue := true
	UserTimelineParams := &twitter.UserTimelineParams{
		ScreenName:      userHandle,
		Count:           400,
		IncludeRetweets: &falsevalue,
		ExcludeReplies:  &truevalue,
	}
	tweets, _, _ := client.Timelines.UserTimeline(UserTimelineParams)
	return tweets
}

func getMostActiveRetweeter(db *gorm.DB, lengthOfTweets int, userHandle string) string {
	db.Exec("ALTER DATABASE twitter CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci;")
	db.Exec("CREATE TABLE `finaltable` LIKE `Retweeters`;")
	db.Exec("INSERT INTO `finaltable` (`User`, `Retweet`) SELECT `User`, `Retweet` FROM `Retweeters` ORDER BY `Retweet` desc;")
	var result Retweeters
	db.Raw("select * from finaltable limit 1").Scan(&result)
	mostActiveRetweeter := result.User
	fmt.Printf("The most active retweeter of the last %+v tweets of %+v is %+v", lengthOfTweets, userHandle, mostActiveRetweeter)
	return mostActiveRetweeter
}

func maxRetweeter(c *gin.Context) {
	db := getDatabase()
	defer db.Close()
	client := getClient(c)
	//  show user recent tweets
	tweets := getTweets(c, client)

	fmt.Printf("Length of user tweet %d\n", len(tweets))
	userHandle := c.Param("userHandle")
	var wg sync.WaitGroup
	lengthOfTweets := 100
	if len(tweets) < 100 {
		lengthOfTweets = len(tweets)
	}
	for i := 0; i < lengthOfTweets; i++ {
		wg.Add(1)
		twitID := tweets[i].ID
		go worker(i, twitID, client, db, &wg)
	}

	wg.Wait()
	mostActiveRetweeter := getMostActiveRetweeter(db, lengthOfTweets, userHandle)
	c.JSON(200, gin.H{
		"maxRetweeter": mostActiveRetweeter,
	})
}

//Worker function which will update the count map by
func worker(id int, twitID int64, client *twitter.Client, db *gorm.DB, wg *sync.WaitGroup) {
	defer wg.Done()
	twits, _, _ := client.Statuses.Retweets(twitID, &twitter.StatusRetweetsParams{Count: 100})
	for _, twit := range twits {
		var retwet Retweeters
		retwitName := twit.User.Name
		db.Raw("select * from retweeters where user=?", retwitName).Scan(&retwet)
		if retwet.User != "" {
			db.Exec("UPDATE Retweeters SET retweet = retweet+1 WHERE user = ?", retwitName)
		} else {
			db.Create(&Retweeters{User: retwitName, Retweet: 1})
		}
	}
}

func main() {
	router := gin.Default()
	router.GET("/twitter/retweets/:userHandle/max", maxRetweeter)
	router.Run(":8080")
}
