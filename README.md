# vader
Code and documentation about Vader sentiment analysis

VaderTest - sample VADER run
  - input: two CSV files (50 Biden tweets from 10 Nov, 50 Trump tweets from 10 Nov)
    - these files were trimmed down to 50 lines and only the tweets
  - files are read in line by line, each row is added to an ArrayList
  - the ArrayList is passed through a Sentiment Analyzer object
  - after running through the SA object, a total compound score, average compound score, and the number of positive, negative, and neutral tweets is printed out for each file
  - note: positive = compound >= .05, negative = compound <= -.05
