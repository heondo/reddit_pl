# reddit_pl
reddit pipeline practice

practicing working with data, python, and aws services.

#### Reddit Data Stream Done
Successfully got my lambda function to run and now have cloudwatch events running every 6 hours.
The code grabs the top posts with its comments and stores them in s3. I am doing every 6 hours so that for an analysis, every post that my API is able to get is at least 18 hours old; I imagine that is the amount of time it takes for most reddit posts lose a lot of traction.

#### Clean and Analyze
Let the data build up for a couple days, create a function to do simple analysis, maybe create a dictionary of the biggest tickers in the industry, do a word count. Cleaning the text, using a html5 request as a trigger to show an analysis. Or whatever else literally. 

Easy so when a user enters an HTML or does something....that I will figure out, access the posts/comments from the past week and with those txt documents, aggregate and drop_duplicates based on post_id for posts and time. Could fix my function so that the date...isn't created within each loop and at the beginning of the lambda function, but really since its every 6 hours all I have to do is clean the date to day+hour.

#### Okay my codes all a mess now, but cleaning up shouldnt be a big deal.
Good progress! Got text files into dynamodb, not fully done with function but class should be able to upload and insert on its own.

### how to clean.

* Fix lambda function, it does not know what classes are so fix that. 
 * All of this could be...done in the same function. Lambda function that gets PRAW api call, cleans it up and throws it into dynamodb + s3 of original text files.
* so basically clean entire...project. yeah.
* once my code...successfully reads in praw data, uploads text file and update-inserts into dynamodb tables based on recency or something.
* Then my database...can easily be queried by me for most updated comments and submissions, do analysis of past week or something?