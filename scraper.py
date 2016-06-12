
import datetime as dt
import functools as ft
import sqlite3
import urllib.parse

from lxml.html import document_fromstring as parse_html
from splinter import Browser

base_url = 'http://www.bahamas.gov.bs/wps/portal/public/?urile=wcm%3apath%3a/MOF_Content/internet/The+Government/Government/The+Government/Legislative/Members+of+Parliament/'
honorifics = ('The Hon.', 'Dr.', 'MP, ')


def scrape_rows(session, rows):
    for row in rows:
        profile_link = urllib.parse.urljoin(base_url,
                                            row.xpath('.//a/@href')[0])
        constituency, island, group = ([*i.xpath('./text()'), ''][0].strip()
                                       for i in row.xpath('./td[position() > 1]'))
        name, = row.xpath('.//a/text()')
        last, first = (i.strip()
                       for i in ft.reduce(lambda s, r: s.replace(r, ''),
                                          honorifics, name).split(','))

        session.visit(profile_link)
        image, = parse_html(session.html)\
            .xpath('//img[@class = "alignLeft sidePicture"]/@src')
        image = urllib.parse.urljoin(base_url, image)
        yield (first + ' ' + last,
               last + ', ' + first,
               last,
               first,
               image,
               group.strip('()'),
               constituency,
               island,
               profile_link)


def gather_people(session):
    session.visit(base_url)
    while True:
        yield from iter(parse_html(session.html)
                        .xpath('//table[@class = "detailTable detailTable_full"]/tbody/tr'))
        next_page = session.find_by_xpath('//a[@title = "Link to next page"]')
        if not next_page:
            break
        next_page.click()


def main():
    with Browser('phantomjs', load_images=False) as browser, \
            sqlite3.connect('data.sqlite') as c:
        people = scrape_rows(browser, tuple(gather_people(browser)))
        c.execute('''\
CREATE TABLE IF NOT EXISTS data
(name, sort_name, family_name, given_name, image,
 'group', constituency, island, source, as_of,
 UNIQUE (name, 'group', constituency, island))''')
        c.executemany('''\
INSERT OR REPLACE INTO data VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
            (lambda date: ((*p, date)
                           for p in people))(dt.date.today().isoformat()))

if __name__ == '__main__':
    main()
