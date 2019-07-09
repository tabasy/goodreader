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
	USER_REVIEW_BASE_URL = 'https://www.goodreads.com/review/list/%d?page=%d&order=d&sort=title&view=reviews'

	rating_cols = ['book_id', 'user_id', 'rate', 'date']
	review_cols = rating_cols #+ ['review']
	book_cols = ['book_id', 'title', 'cover_url', 'rate_avg', 'rate_no', 'author_name', 'author_id']
				# , 'author_name_2', 'author_id_2']
	
	def __init__(self, **kwargs):
		
		super().__init__(**kwargs)
		
		self.SEED = int(getattr(self, 'seed', '637699'))
		self.MIN_RATE_PER_BOOK = int(getattr(self, 'min_rate_per_book', '5'))
		self.REVIEW_ONLY = getattr(self, 'review_only', 'false') == 'true'
		self.MAX_USER = int(getattr(self, 'max_user', '1000'))
		self.MAX_BOOK = int(getattr(self, 'max_book', '100'))
		self.MAX_REVIEW_PAGES = int(getattr(self, 'max_page', '2'))
		self.BOOKS_OUT = getattr(self, 'books_out', 'books.csv')
		self.RATES_OUT = getattr(self, 'rates_out', 'rates.csv')
		
		self.CONTINUE = getattr(self, 'continue', 'false') == 'true'
		
		self.VALID_LANGS = ['Persian']
		
		if self.CONTINUE:
			
			with open(self.BOOKS_OUT, 'r', encoding='utf8') as f:
				books_df = pd.read_csv(f, sep='\t', error_bad_lines=False)
				self.book_ids = set(books_df['book_id'].values)
				
			with open(self.RATES_OUT, 'r', encoding='utf8') as f:
				rates_df = pd.read_csv(f, sep='\t', error_bad_lines=False)
				self.user_ids = set(rates_df['user_id'].values)
		else:
			self.user_ids = set()
			self.book_ids = set()
			
			if self.REVIEW_ONLY:
				rates_header = pd.DataFrame(columns=RatesSpider.review_cols)
			else:
				rates_header = pd.DataFrame(columns=RatesSpider.rating_cols)
			
			with open(self.RATES_OUT, 'w', encoding='utf8') as f:
				rates_header.to_csv(f, sep='\t', line_terminator='\n', header=True, index=False)
			
			books_header = pd.DataFrame(columns=RatesSpider.book_cols)
			with open(self.BOOKS_OUT, 'w', encoding='utf8') as f:
				books_header.to_csv(f, sep='\t', line_terminator='\n', header=True, index=False)
		
		self.ignored = {'invalid_lang': 0, 'duplicate_book': 0, 'obscure': 0, 'max_book': 0,
		                'duplicate_user': 0, 'max_user': 0}
		
	def start_requests(self):
		
		url = self.BOOK_BASE_URL % self.SEED
		yield scrapy.Request(url=url, callback=self.parse_book_page, meta={'book_id': self.SEED})
	
	def parse_book_page(self, response):
		
		# get book info
		book_id = response.meta.get('book_id')
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
		
		self.book_ids.add(int(book_id))
		
		df = pd.DataFrame(columns=RatesSpider.book_cols)
		df.loc[0] = [book_id, title, cover_url, rate_avg, ratings_no, author_names[0], author_ids[0]]
		
		with open(self.BOOKS_OUT, 'a', encoding='utf8') as f:
			df.to_csv(f, sep='\t', line_terminator='\n', header=False, index=False)
		
		for rate, page in product(range(5), range(1)):
			if self.REVIEW_ONLY:
				yield scrapy.Request(self.REVIEW_BASE_URL % (int(book_id), page+1, rate+1),
				                     callback=self.parse_reviews, meta={'book_id': int(book_id)})
			else:
				yield scrapy.Request(self.RATING_BASE_URL % (int(book_id), page+1, rate+1),
				                     callback=self.parse_reviews, meta={'book_id': int(book_id)})
		
	def parse_reviews(self, response, *args, **kwargs):
		
		book_id = response.meta.get('book_id')
		raw_str = response.text
		
		start = raw_str.find(', ') + 3
		end = raw_str.rfind('"')
		valid_html = raw_str[start:end]\
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
			
			new_users.append(int(user_id))
			
			if len(review) > 1:
				review = review[-1]
			elif len(review) == 1:
				review = review[0]
			else:
				review = ''
			
			if self.REVIEW_ONLY:
				df.loc[len(df)] = [int(book_id), int(user_id), int(rate), date] # + [review]
			else:
				df.loc[len(df)] = [int(book_id), int(user_id), int(rate), date]
		
		with open(self.RATES_OUT, 'a', encoding='utf8') as f:
			df.to_csv(f, sep='\t', line_terminator='\n', header=False, index=False)
		
		for u in new_users:
			if self.validate_user(u):
				self.user_ids.add(u)
				for page in range(3):
					yield scrapy.Request(url=self.USER_REVIEW_BASE_URL % (u, page + 1), callback=self.parse_user_page)
			
	def parse_user_page(self, response):
		
		book_ids = response.css('td.title a::attr(href)').re('[0-9]+')
		
		for b in book_ids:
			if int(b) not in self.book_ids:
				yield scrapy.Request(self.BOOK_BASE_URL % int(b), callback=self.parse_book_page, meta={'book_id': int(b)})

	def validate_book(self, book_id, language='Persian', ratings_no=1000):
		
		if language not in self.VALID_LANGS:
			self.ignored['invalid_lang'] += 1
			print(f'-- ignored bcz of invalid_lang, total {self.ignored["invalid_lang"]} ignored bcz of this!')
			return False
		if book_id in self.book_ids:
			self.ignored['duplicate_book'] += 1
			print(f'-- ignored bcz of duplicate_book, total {self.ignored["duplicate_book"]} ignored bcz of this!')
			return False
		if ratings_no < self.MIN_RATE_PER_BOOK:
			self.ignored['obscure'] += 1
			print(f'-- ignored bcz of obscure, total {self.ignored["obscure"]} ignored bcz of this!')
			return False
		if len(self.book_ids) >= self.MAX_BOOK:
			self.ignored['max_book'] += 1
			print(f'-- ignored bcz of max_book, total {self.ignored["max_book"]} ignored bcz of this!')
			return False
		print(f'>> adding {len(self.book_ids)+1}-th book!')
		return True
	
	def validate_user(self, user_id):
		
		if user_id in self.user_ids:
			self.ignored['duplicate_user'] += 1
			print(f'-- ignored bcz of duplicate_user, total {self.ignored["duplicate_user"]} ignored bcz of this!')
			return False
		if len(self.user_ids) >= self.MAX_USER:
			self.ignored['max_user'] += 1
			print(f'-- ignored bcz of max_user, total {self.ignored["max_user"]} ignored bcz of this!')
			return False
		print(f'>> adding {len(self.user_ids)+1}-th user!')
		return True