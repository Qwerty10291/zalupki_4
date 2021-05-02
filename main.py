import asyncio
from typing import List
import aiohttp
from aiohttp import ClientConnectionError
import re
import sys
import csv
from lxml.html import document_fromstring
from datetime import datetime, timedelta
import atexit


class Participant:
    def __init__(self, id, inn, name, link) -> None:
        self.id = id
        self.inn = inn
        self.name = name
        self.link = link
        self.mail = ''


class Parser:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:87.0) Gecko/20100101 Firefox/87.0', }
        self.set_up()
        atexit.register(self.doc_normalizer)

    def set_up(self):
        self.main_link = 'https://zakupki.gov.ru'
        self.search_link = 'https://zakupki.gov.ru/epz/eruz/search/results.html'
        self.params = self.parse_parameters('filters.txt')

    def parse_parameters(self, filename: str):
        file = open(filename, 'r', encoding='utf-8')
        data = list(map(lambda x: re.split(
            r'\s*\=\s*', x.replace('\n', '')), file.readlines()))
        params = dict((e[0], e[1]) for e in data)
        if 'date_from' in params:
            self.date_from = datetime.strptime(
                params.pop('date_from'), '%d.%m.%Y')
        else:
            print('Нет параметра даты начала.')
            input()
            sys.exit()
        if 'date_to' in params:
            date = params.pop('date_to')
            if date == 'today':
                self.date_to = datetime.now()
            else:
                self.date_to = datetime.strptime(date, '%d.%m.%Y')
        if self.date_from > self.date_to:
            print('дата начала не может быть позже даты конца')
            input()
            sys.exit()
        if 'days' in params:
            self.date_diff = timedelta(int(params.pop('days')))
        else:
            self.date_diff = timedelta(14)
        if 'workers' in params:
            self.workers = int(params.pop('workers'))
        else:
            self.workers = 3
        if 'out_filename' in params:
            self.filename = params.pop('out_filename')
            self.file = open(self.filename, 'w', encoding='utf-8')
        else:
            self.filename = 'out.csv'
            self.file = open('out.csv', 'w', encoding='utf-8')
        if 'max_retries' in params:
            self.max_retries = int(params.pop('max_retries'))
        else:
            self.max_retries = 20
        self.writer = csv.writer(self.file, delimiter=';', quotechar='"')
        return params

    def start(self):
        asyncio.run(self.parse())

    async def parse(self):
        async with aiohttp.ClientSession(headers=self.headers) as session:
            for i in range(1, (self.date_to - self.date_from) // self.date_diff):
                self.params['registryDateTo'] = self.date_to.__sub__(
                    self.date_diff * i).strftime('%d.%m.%Y')
                self.params['registryDateFrom'] = self.date_to.__sub__(
                    self.date_diff * i).__sub__(self.date_diff).strftime('%d.%m.%Y')
                pages = await self.load_pages_count(session)
                print(
                    f'{self.params["registryDateTo"]}-{self.params["registryDateFrom"]}: {int(pages)}')
                if not pages:
                    continue
                tasks = [self.search_pages(session, i)
                         for i in range(1, pages + 1)]
                users_links = await asyncio.gather(*tasks)
                print('Страниц:', len(users_links))
                for i in range(self.workers, len(users_links) + 1, self.workers):
                    print(f'страницы: {i - self.workers + 1}-{i + 1}')
                    tasks = [self.parse_email_pages(
                        session, page) for page in users_links[i - self.workers:i]]
                    users = await asyncio.gather(*tasks, return_exceptions=False)
                    self.csv_writer(users)
                if len(users_links) % self.workers != 0:
                    tasks = [self.parse_email_pages(session, page) for page in users_links[len(
                        users_links) - len(users_links) % self.workers:]]
                    users = await asyncio.gather(*tasks, return_exceptions=False)
                    self.csv_writer(users)
            self.doc_normalizer()

    async def parse_email_pages(self, session: aiohttp.ClientSession, page: list):
        for user in page:
            try:
                data = await self._get_request(session, user.link)
                mail = re.findall(r'[^\s\>]+@.+\.\w+', data)
                if mail:
                    user.mail = mail[0]
            except Exception as msg:
                print('email parse error: ', msg)
        return page

    async def load_pages_count(self, session: aiohttp.ClientSession):
        data = await self._get_request(session, self.search_link, params=self.params)
        doc = document_fromstring(data)
        pages = doc.xpath('//ul[@class="pages"]/li[last()]/a/span/text()')
        if pages:
            return int(pages[0])
        else:
            return False

    async def search_pages(self, session: aiohttp.ClientSession, page: int) -> List[List[Participant]]:

        params = self.params.copy()
        params['pageNumber'] = str(page)
        data = await self._get_request(session, self.search_link, params=params)
        doc = document_fromstring(data)
        try:
            users = self.parse_page(doc)
        except Exception as msg:
            users = []
            print(f'Ошибка парсинга страницы {page}: {msg}')
        return users

    def parse_page(self, doc) -> List[Participant]:
        containers = doc.xpath(
            '//div[@class="search-registry-entry-block box-shadow-search-input"]')
        users = []
        for container in containers:
            try:
                idc = container[0].xpath(
                    './/div[@class="registry-entry__header-mid__number"]/a')
                id = self._normalizer(idc[0].xpath(
                    './text()')[0].replace('№', ''))
                link = self._normalizer(
                    self.main_link + idc[0].xpath('./@href')[0])
                inn = self._normalizer(container[0].xpath(
                    './/div[@class="registry-entry__body-value"]/text()')[0])
                name = self._normalizer(container[0].xpath(
                    './/div[@class="registry-entry__body-href"]/a/text()')[0])
                users.append(Participant(id, inn, name, link))
            except:
                pass
        return users

    def csv_writer(self, users: list):
        for page in users:
            for user in page:
                self.writer.writerow(
                    [user.id, user.name, user.inn, user.mail, user.link])

    async def _get_request(self, session: aiohttp.ClientSession, url, params={}, retries=0) -> str:
        try:
            async with session.get(url, allow_redirects=True, params=params) as query:
                if query.status != 200:
                    print(f'try: {retries}')
                    if query.status == 503:
                        if retries > self.max_retries:
                            raise ClientConnectionError(
                                'Слишком много неудачных попыток')
                        await asyncio.sleep(retries + 1)
                        return await self._get_request(session, url, params, retries=retries + 1)
                    elif query.status == 404:
                        raise ClientConnectionError(
                            f'Страница: {url} не найдена')
                    else:
                        return ClientConnectionError(f'{url}: code {query.status}')
                return await query.text()
        except aiohttp.ServerDisconnectedError:
            await asyncio.sleep(1)
            print(f'tryd: {retries}')
            if retries > 10:
                raise ClientConnectionError(
                    'Слишком много неудачных попыток')
            return await self._get_request(session, url, params, retries=retries + 1)
        except Exception as msg:
            print('load page error:', msg)

    def _normalizer(self, text: str) -> str:
        return text.replace('\n', '').replace('\xa0', '').strip()

    def doc_normalizer(self):
        self.file.close()
        file = open(self.filename, 'r', encoding='utf-8')
        data = file.read().replace('\n\n', '\n')
        file.close()
        open(self.filename, 'w', encoding='utf-8').write(data)


parser = Parser()
loop = asyncio.get_event_loop()
users = loop.run_until_complete(parser.parse())
