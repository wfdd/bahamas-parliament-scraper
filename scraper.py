
import csv
import datetime as dt
import functools as ft
import sqlite3
from urllib.parse import urlencode, urljoin
from urllib.request import urlopen
import sys

from lxml.html import document_fromstring as parse_html
from splinter import Browser

base_url = 'http://www.bahamas.gov.bs/wps/portal/public/'
list_urls = '?1dmy&urile=wcm%3apath%3a/MOF_Content/internet/The+Government/Government/The+Government/Legislative/Members+of+Parliament/', \
            '?1dmy&urile=wcm%3apath%3a/MOF_Content/internet/All+Contacts/Directory+of+Contacts/Listed+by+Parliamentary+Secretaries/'

honorifics = ('The Hon.', 'Dr.', 'MP, ')


def extract_birth_date(text):
    try:
        text = next(p.text_content() for p in text
                    if 'born' in p.text_content())
    except StopIteration:
        return
    with urlopen('http://nlp.stanford.edu:8080/sutime/process',
                 data=urlencode({'q': text, 'rules': 'english'}).encode()) as r:
        date, = parse_html(r.read())\
            .xpath('//h3[text() = "Temporal Expressions"]'
                   '/following-sibling::table[1]//tr[2]/td[2]/text()') or (None,)
        if not date:
            print('Unable to extract birth date from {!r}'.format(text),
                  file=sys.stderr)
        return date


def scrape_row(session, row):
    profile_url = urljoin(base_url, row.xpath('.//a/@href')[0])
    constituency, island, group = (None,) * 3
    if 'Parliamentary Secretaries' not in row.xpath('string(//title)'):
        constituency, island, group = ((*i.xpath('./text()'), '')[0].strip()
                                       for i in row.xpath('./td[position() > 1]'))
    name, = row.xpath('.//a/text()')
    last, first = (i.strip()
                   for i in ft.reduce(lambda s, r: s.replace(r, ''),
                                      honorifics, name).split(','))

    session.visit(profile_url)
    html = parse_html(session.html)
    image, = html.xpath('//img[@class = "alignLeft sidePicture"]/@src')
    image = urljoin(base_url, image)
    return (first + ' ' + last,
            last + ', ' + first,
            last,
            first,
            extract_birth_date(html.xpath('//div[text() = "Biography"]'
                                          '/following-sibling::p')),
            image,
            group and group.strip('()'),
            constituency,
            island,
            profile_url)


def collect_rows(session):
    for list_url in list_urls:
        session.visit(urljoin(base_url, list_url))
        while True:
            yield from iter(parse_html(session.html)
                            .xpath('//table[@class = "detailTable detailTable_full"]/tbody/tr'))

            next_page = session.find_by_xpath('//a[@title = "Link to next page"]')
            if not next_page:
                break
            next_page.click()


def main():
    with Browser('phantomjs', load_images=False) as session, \
            sqlite3.connect('data.sqlite') as cursor, \
            open('elected-2012.csv') as elected:
        cursor.execute('''\
CREATE TABLE IF NOT EXISTS data
(name, sort_name, family_name, given_name, birth_date, image,
 'group', constituency, island, source, as_of,
 UNIQUE (name, 'group', constituency, island))''')
        cursor.executemany('''\
INSERT OR REPLACE INTO data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (lambda date: ((*p, date)
                           for p in (scrape_row(session, r)
                                     for r in tuple(collect_rows(session)))
                           ))(dt.date.today().isoformat()))
        cursor.execute('''\
CREATE TABLE IF NOT EXISTS elected
(name, area, 'group', term, UNIQUE (name, area, 'group', term))''')
        cursor.executemany('''\
INSERT OR REPLACE INTO elected VALUES (?, ?, ?, ?)''',
            tuple(csv.reader(elected))[1:])

if __name__ == '__main__':
    main()
