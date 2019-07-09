# Goodreader

Goodreads scraper with scrapy and requests-future and without selenium :)
Scraped data is meant to be specialized for a book recommendation system, but may be helpful for other applications.

# What is the output?

The crawler starts from a book page, gets book info and rates/reviews. Users who rated this book, have rated many other books and so on...

Book info are stored in `/goodreader/books.csv` in this format (tab separated):

book_id	| title	| cover_url	| rate_avg | rate_no | author_name | author_id
--- | --- | --- | --- | --- | --- | ---
637699 |	یکی بود و یكی نبود | url.jpg |	3.72	| 943	| Mohammad Ali Jamalzadeh |	607716
426025 | یک گل سرخ برای امیلی	| url.jpg	| 4.06 |	24436 |	William Faulkner |3535

Rates are saved in `/goodreader/rates.csv` like this (tab separated):

book_id | user_id | rate | date 
--- | --- | --- | --- 
637699 |	614778 |	5 |	Jun 02, 2008
637699 |	21030932 |	3 |	Nov 10, 2015
637699 |	55469702 |	4 |	May 26, 2018

# How to use?

After installing requirements, run this in `/goodreader` directory (not root!)
```
scrapy crawl rates
```
You will have output in `/goodreader/books.csv` and `/goodreader/rates.csv`
