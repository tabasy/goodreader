# Goodreader

Goodreads scraper with scrapy and requests-future and without selenium :)
Scraped data is meant to be specialized for a book recommendation system, but may be helpful for other applications.

# What is the output?

The crawler starts from a book page, gets book info and rates/reviews. Users who rated this book, have rated many other books and so on...

Book info are stored in `/goodreader/books.csv` in this format (tab separated):
```
book_id   title   cover_url   rate_avg    rate_no   author_name   author_id
```
Rates are saved in `/goodreader/rates.csv` like this (tab separated):
```
book_id   user_id   rate    date
```

# How to use?

After installing requirements, run this in `/goodreader` directory (not root!)
```
scrapy crawl rates
```
You will have output in `/goodreader/books.csv` and `/goodreader/rates.csv`
