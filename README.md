# Data Engineering - Data Lakes with Spark

## Contents
1. [Introduction](#introduction)
2. [Setup](#setup)
3. [Datasets](#datasets)
4. [Data Model](#data-model)
7. [References](#references)
8. [License](#license)
9. [Author](#author)


## Introduction

This is a project to demonstrate how to create a data lake using Spark. This project is part of the
[Data Engineering]() nano-degree program at Udacity.

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

The project consists in  building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. 

## Setup

Submit the etl.py script to your spark cluster.
Run the following command to submit the job to spark:

    $ spark-submit etl.py

In order to have access to the S3 bucket, you need to configure your AWS credentials.
Create a dl.cfg file with the following content:

    [KEYS]
    aws_access_key_id = <your access key>
    aws_secret_access_key = <your secret key>


## Datasets

The datasets used in this project are the following:

1. **song_data** - JSON file with metadata on songs.
The **song data** is stored in JSON format. It contains the following fields:
   1. `song_id` - a unique ID for each song
   2. `title` - the title of the song
   3. `artist_id` - the ID of the artist
   4. `artist_name` - the name of the artist
   5. `artist_location` - the location of the artist
   6. `artist_latitude` - the latitude of the artist
   7. `artist_longitude` - the longitude of the artist
   8. `year` - the year the song was released
   9. `duration` - the length of the song in seconds
   10. `num_songs` - the number of songs by the artist


2. **log_data** - JSON file with metadata on user activity.
   The **log data** is stored in JSON format. It contains the following fields:
   11. `artist` - the name of the artist
   12. `auth` - the authentication method used
   13. `firstName` - the first name of the user
   14. `gender` - gender of the user
   15. `itemInSession` - number of items in the session
   16. `lastName` - the last name of the user
   17. `lenght` - length of the session
   18. `level` - level of the user
   19. `location` - location of the user
   20. `method` - the method used to access the data
   21. `page` - the page accessed
   22. `registration` - the registration date of the user
   23. `sessionId` - the ID of the session
   24. `song` - the name of the song
   25. `status` - the status of the request
   26. `ts` - the timestamp of the request
   27. `userAgent` - the user agent of the user
   28. `userId` - the ID of the user

# Data Model
The spark job will create a data lake in S3. The data lake will contain the following dimensional tables:

1. **artists** - the artists table
2. **users** - the users table
3. **time** - the time table
4. **songs** - the songs table

And the following factual table:

5. **songplays** - the songplays table

# References

The following references were used to create this project:

1. [Data Engineering Nano-degree](https://www.udacity.com/course/data-engineering-etl-pipeline--ud853)
2. [Spark Documentation](https://spark.apache.org/docs/latest/sql-programming-guide.html)
3. [Spark SQL Programming Guide](https://spark.apache.org/docs/latest/sql-programming-guide.html)

# License

This project is licensed under the MIT License.

## Author
- [Erick Escobar.](https://www.linkedin.com/in/erick-escobar-892b20103/)