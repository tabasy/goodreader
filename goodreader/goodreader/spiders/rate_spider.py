from time import sleep

import scrapy
import pandas as pd

from scrapy.http import HtmlResponse
from requests_futures.sessions import FuturesSession
from itertools import product


class RatesSpider(scrapy.Spider):
	
	name = 'rates'
	
	BASE_URL = 'https://www.goodreads.com'
	USER_BASE_URL = BASE_URL + '/user/show/%d'
	AUTHOR_BASE_URL = BASE_URL + '/author/show/%d'
	BOOK_BASE_URL = BASE_URL + '/book/show/%d._'
	REVIEW_BASE_URL = BASE_URL + '/book/reviews/%d?language_code=fa&page=%d&rate=%dsort=date_added'
	RATING_BASE_URL = BASE_URL + '/book/reviews/%d?page=%d&rate=%dsort=date_added'
	USER_REVIEW_BASE_URL = 'https://www.goodreads.com/review/list/%d?page=%d&sort=date_read&view=reviews'

	rating_cols = ['book_id', 'user_id', 'rate', 'date']
	review_cols = rating_cols + ['review']
	book_cols = ['book_id', 'title', 'cover_url', 'rate_avg', 'rate_no', 'author_name', 'author_id']
				# , 'author_name_2', 'author_id_2']
	
	def __init__(self, **kwargs):
		
		super().__init__(**kwargs)
		
		self.MIN_RATE_PER_BOOK = 5
		self.REVIEW_ONLY = False
		self.MAX_USER = 100
		self.MAX_BOOK = 100
		
		self.VALID_LANGS = ['Persian']
		
		self.user_ids = set()
		self.author2id, self.id2author = {}, {}
		self.book2id, self.id2book = {}, {}
		self.book2author = {}
		
		if self.REVIEW_ONLY:
			rates_header = pd.DataFrame(columns=RatesSpider.review_cols)
		else:
			rates_header = pd.DataFrame(columns=RatesSpider.rating_cols)
		
		with open('rates.csv', 'w', encoding='utf8') as f:
			rates_header.to_csv(f, sep='\t', line_terminator='\n', header=True, index=False)
		
		books_header = pd.DataFrame(columns=RatesSpider.book_cols)
		with open('books.csv', 'w', encoding='utf8') as f:
			books_header.to_csv(f, sep='\t', line_terminator='\n', header=True, index=False)
		
		self.session = FuturesSession()

	def start_requests(self):
		
		urls = [self.BOOK_BASE_URL % 637699, ]
		
		for url in urls:
			yield scrapy.Request(url=url, callback=self.parse_book_page)
	
	def parse_book_page(self, response):
		
		# get book info
		book_id = response.css('link[rel="canonical"]::attr(href)').re('[0-9]+')[0]
		title = response.css('#bookTitle::text').get().strip()
		language = response.css('div[itemprop="inLanguage"]::text').get()
		
		cover_url = response.css('#coverImage::attr(src)').get()
		author_ids = response.css('#bookAuthors a.authorName::attr(href)').re('[0-9]+')     # maybe multiple
		author_names = response.css('span[itemprop="name"]::text').getall()                 # maybe multiple
		
		description = response.css('#description span::text').get()                         # maybe multiple
		rate_avg = response.css('span[itemprop="ratingValue"]::text').get().strip().strip()
		ratings_no = response.css('meta[itemprop="ratingCount"]::attr(content)').get().strip()
		reviews_no = response.css('meta[itemprop="reviewCount"]::attr(content)').get().strip()
		
		if not self.validate_book(int(book_id), language, int(ratings_no)):
			return
			
		self.book2id[title] = int(book_id)
		self.id2book[int(book_id)] = title
		
		df = pd.DataFrame(columns=RatesSpider.book_cols)
		df.loc[0] = [book_id, title, cover_url, rate_avg, ratings_no, author_names[0], author_ids[0]]
		
		with open('books.csv', 'a', encoding='utf8') as f:
			df.to_csv(f, sep='\t', line_terminator='\n', header=False, index=False)
			
		new_users = self.get_reviews_extract_users(int(book_id))
		
		for u in new_users:
			if self.validate_user(u):
				yield scrapy.Request(url=self.USER_REVIEW_BASE_URL % (u, 1), callback=self.parse_user_page)
				self.user_ids.add(u)
	
	def get_reviews_extract_users(self, book_id):
		
		futures = []
		
		for rate, page in product(range(5), range(1)):
			if self.REVIEW_ONLY:
				future = self.session.get(self.REVIEW_BASE_URL % (book_id, page + 1, rate + 1))
			else:
				future = self.session.get(self.RATING_BASE_URL % (book_id, page + 1, rate + 1))
			futures.append(future)
		
		all_new_users = []
		
		for resp in futures:
			new_users = self.parse_reviews(book_id, resp)
			all_new_users += new_users
	
		return all_new_users
	
	def parse_reviews(self, book_id, future, *args, **kwargs):
		
		r = future.result().content.decode('utf8')
		
		start = r.find(', ') + 3
		end = r.rfind('"')
		valid_html = r[start:end]\
			.replace('\\"', '"').replace('\\"', '"').replace('\\"', '"')\
			.replace('\\n', '')\
			.replace('\\u003c', '<')\
			.replace('\\u003e', '>')
		
		response = HtmlResponse(url='url', body=valid_html, encoding='utf8')
		
		new_users = []
		
		if self.REVIEW_ONLY:
			df = pd.DataFrame(columns=RatesSpider.review_cols)
		else:
			df = pd.DataFrame(columns=RatesSpider.rating_cols)
			
		for item in response.css('div.review'):
			
			rate = len(item.css('span.p10').getall())
			user_id = item.css('a.user::attr(href)').re('[0-9]+')[0]
			date = item.css('a.reviewDate::text').get()
			review = item.css('div.reviewText > span > span').getall()
			
			if self.validate_user(int(user_id)):
				new_users.append(int(user_id))
			
			if len(review) > 1:
				review = review[-1]
			elif len(review) == 1:
				review = review[0]
			else:
				review = ''
			
			if self.REVIEW_ONLY:
				df.loc[len(df)] = [int(book_id), int(user_id), int(rate), date, review]
			else:
				df.loc[len(df)] = [int(book_id), int(user_id), int(rate), date]
		
		with open('rates.csv', 'a', encoding='utf8') as f:
			df.to_csv(f, sep='\t', line_terminator='\n', header=False, index=False)
			
		return new_users
		
	def parse_user_page(self, response):
		
		book_ids = response.css('td.title a::attr(href)').re('[0-9]+')
		
		for b in book_ids:
			if self.validate_book(int(b)):
				yield scrapy.Request(self.BOOK_BASE_URL % int(b), callback=self.parse_book_page)
		
	def validate_book(self, book_id, language='Persian', ratings_no=1000):
		return language in self.VALID_LANGS and book_id not in self.id2book and \
		       ratings_no > self.MIN_RATE_PER_BOOK and len(self.id2book) < self.MAX_BOOK
	
	def validate_user(self, user_id):
		return user_id not in self.user_ids and len(self.user_ids) < self.MAX_USER