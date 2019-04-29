import os
os.chdir('/home/windr/ResilioSync/GitHub/reco-engine/code/scraping')

import sys
sys.path.append('../scraping')
sys.path.append('../tools')

import time
import asyncio
import aiohttp
import logging
import requests
import db_connect
from pprint import pprint
from bs4 import BeautifulSoup
from functools import partial
from xml.etree import ElementTree
from multiprocessing import Pool, Manager
from get_existing_urls import get_existing_urls


async def fetch_mv_urls(url, session, mv_urls: set):
    to = aiohttp.ClientTimeout(connect=2.0, sock_read=2.0)
    top_part = 'https://www.rottentomatoes.com/m/'
    async with session.get(url, timeout=to) as response:
        mv_urls.update((child[0].text for child in ElementTree.fromstring(await response.read())
                        if child[0].text.startswith(top_part)
                        and child[0].text[len(top_part):].count('/') <= 1))
    return


async def proc1(movie_urls: set):
    # Get Sitemaps
    origin = 'https://www.rottentomatoes.com/sitemap.xml'
    sitemaps = list(child[0].text for child in ElementTree.fromstring(requests.get(origin).content))
    # Run 1
    async with aiohttp.ClientSession() as session1:
        tasks1 = list(asyncio.ensure_future(fetch_mv_urls(url=url, session=session1, mv_urls=movie_urls))
                      for url in sitemaps)
        await asyncio.gather(*tasks1)
    # print(f'\r\n{len(movie_urls)} urls')
    return


def get_movie_detail(lock, furl):
    # Define output variable
    output = dict()
    # Movie_URL
    output['Movie_URL'] = furl[len('https://www.rottentomatoes.com'):]
    # Load Page (max 5 times)
    page = None
    error = 0
    for t in range(5):
        try:
            error = 0
            page = requests.get(furl, timeout=5)
        except requests.exceptions.ConnectionError:
            error = 1
            pass
        except requests.exceptions.Timeout:
            error = 2
            pass
        else:
            break
    if not page:  # Page load error
        lock.acquire()
        print(f'\033[1;31m\t- Bad page ({page.status_code}, {error}). Moving on | \033[30m{furl}')
        lock.release()
        return False
    else:
        # Brew soup
        soup = BeautifulSoup(page.content, "html.parser")
        # FIND Franchise/Flag
        try:
            output['Franchise'] = soup.select_one('div.franchiseLink').select_one('em').text.strip()
        except AttributeError:
            pass
        # FIND Movie_Name
        try:
            output['Movie_Name'] = soup.select_one(
                'h1.mop-ratings-wrap__title.mop-ratings-wrap__title--top').text.strip()
        except AttributeError:
            pass
        # FIND Critics_Consensus
        try:
            output['Critics_Consensus'] = soup.select_one(
                'p.mop-ratings-wrap__text.mop-ratings-wrap__text--concensus').text.strip()
        except AttributeError:
            pass
        # FIND TomatoMeter & AudienceScore
        try:
            rate_panels = soup.select('div.mop-ratings-wrap__half')
        except AttributeError:
            pass
        else:
            for rate_panel in rate_panels:
                if rate_panel.find(href='#contentReviews'):
                    try:
                        output['Tomato_Meter'] = rate_panel.select_one('span.mop-ratings-wrap__percentage').text.strip()
                    except AttributeError:
                        pass
                    try:
                        output['Tomato_Meter_rc'] = rate_panel.select_one('small.mop-ratings-wrap__text--small').text.strip()
                    except AttributeError:
                        pass
                elif rate_panel.find(href='#audience_reviews'):
                    try:
                        output['Audience_Score'] = rate_panel.select_one('span.mop-ratings-wrap__percentage').text[:-9].strip()
                    except AttributeError:
                        pass
                    try:
                        output['Audience_Score_ur'] = rate_panel.select_one('small.mop-ratings-wrap__text--small').text.strip()
                    except AttributeError:
                        pass
                else:
                    pass
        # Movie Info
        try:
            mi = soup.select_one('section.panel.panel-rt.panel-box.movie_info.media')
        except AttributeError:
            pass
        else:
            # FIND MI_Description
            try:
                output['MI_Description'] = mi.find(id='movieSynopsis').text.strip()
            except AttributeError:
                pass
            for mi_item in mi.select('li.meta-row.clearfix'):
                try:
                    mi_item_label = mi_item.select_one('div.meta-label.subtle').text.strip()[:-1]
                except AttributeError:
                    pass
                    continue
                else:
                    # FIND MI_Rating
                    if mi_item_label == 'Rating':
                        try:
                            output['MI_Rating'] = mi_item.select_one('div.meta-value').text.strip()
                        except AttributeError:
                            pass
                    # FIND MI_Genre
                    elif mi_item_label == 'Genre':
                        try:
                            output['MI_Genre'] = tuple(int(genre['href'][24:]) for genre in mi_item.select('a'))
                        except AttributeError:
                            pass
                    # FIND MI_Directed_by
                    elif mi_item_label == 'Directed By':
                        try:
                            output['MI_Director'] = tuple(director.text.strip() for director in mi_item.select('a'))
                        except AttributeError:
                            try:
                                output['MI_Director'] = mi_item.select_one('div.meta-value').text.strip()
                            except AttributeError:
                                pass
                    # FIND MI_Written_by
                    elif mi_item_label == 'Written By':
                        try:
                            output['MI_Writer'] = tuple(writer.text.strip() for writer in mi_item.select('a'))
                        except AttributeError:
                            try:
                                output['MI_Writer'] = mi_item.select_one('div.meta-value').text.strip()
                            except AttributeError:
                                pass
                    # FIND MI_In_Theaters
                    elif mi_item_label == 'In Theaters':
                        try:
                            output['MI_In_Theaters_0'] = mi_item.select_one('time')['datetime']
                            output['MI_In_Theaters_1'] = mi_item.select_one('time').text.strip()
                        except AttributeError:
                            pass
                    # FIND MI_On_Disc/Streaming
                    elif mi_item_label == 'On Disc/Streaming':
                        try:
                            output['MI_On_Disc_0'] = mi_item.select_one('time')['datetime']
                            output['MI_On_Disc_1'] = mi_item.select_one('time').text.strip()
                        except AttributeError:
                            pass
                    # FIND MI_Runtime
                    elif mi_item_label == 'Runtime':
                        try:
                            output['MI_Runtime_0'] = mi_item.select_one('time')['datetime']
                            output['MI_Runtime_1'] = mi_item.select_one('time').text.strip()
                        except AttributeError:
                            pass
                    # FIND MI_Studio
                    elif mi_item_label == 'Studio':
                        try:
                            output['MI_Studio'] = mi_item.select_one('div.meta-value').text.strip()
                        except AttributeError:
                            pass
                    # FIND MI_Box_Office
                    elif mi_item_label == 'Box Office':
                        try:
                            output['MI_Box_Office'] = mi_item.select_one('div.meta-value').text.strip()
                        except AttributeError:
                            pass
                    # Catch New Labels
                    else:
                        input('New MI label "{}". Please acknowledge:\n'.format(mi_item_label))
        # FIND Watch_It_Now/Streaming_Services
        try:
            output['Watch_It_Now'] = tuple(m_link.select_one('div.logo').text.strip()
                                           for m_link in soup.select_one('div.movie_links').select('a'))
        except AttributeError:
            pass
        # FIND Casts(Name+As)
        try:
            output['Casts'] = tuple(tuple(
                [cast.select_one('div.media-body').select_one('span').text.strip(),
                 cast.select_one('span.characters.subtle.smaller').text[3:].strip()])
                                    for cast in soup.select_one('div.castSection').select(
                'div.cast-item.media.inlineBlock'))
        except AttributeError:
            pass
        # Return output as a dictionary for database insertion
        return output


def main(db_cred, worker_count=32):
    # Record Overall Start Time
    t0 = time.time()

    # Record Procedure 1 Start Time
    start_time1 = time.time()
    # Print start status
    print('\033[1;34m\r\nProcedure 1 started\r\n\033[30m')
    # Procedure 1: Execute proc1() (Obtaining all movie urls from sitemap)
    movie_urls = set()
    asyncio.get_event_loop().run_until_complete(asyncio.ensure_future(proc1(movie_urls)))
    # Check with records in the Database and remove duplicates
    existing_url_list = get_existing_urls(db_connect.get_collection(conn_detail=db_cred))
    movie_urls = set(url for url in movie_urls
                     if url[len('https://www.rottentomatoes.com'):] not in existing_url_list)
    # Print end status
    print(f'\033[1;32mProcedure 1 finished | {len(movie_urls)} urls obtained')
    # Print runtime
    print(f'Runtime: {int(time.time() - start_time1)} seconds\r\n\033[30m')

    # Record Procedure 2 Start Time
    start_time2 = time.time()
    # Print start status
    print('\033[1;34m\r\nProcedure 2 started\r\n\033[30m')
    # Get a Lock
    lock = Manager().Lock()
    # Procedure 2: Scrape on!
    results = list(result for result in
                   Pool(worker_count).map(partial(get_movie_detail, lock), movie_urls) if result)
    # Insert data into database
    if len(results) > 0:
        db_connect.get_collection(conn_detail=db_cred).insert_many(results)
    # Print end status
    print(f'\033[1;32mProcedure 2 finished | {len(results)} inserted')
    # Print runtime
    print(f'Runtime: {int(time.time() - start_time2)} seconds\r\n\033[30m')

    # Print Overall Runtime
    print(f'\033[1;32m\r\nTotal runtime: {round(time.time() - t0, 1)} seconds\033[30m')
    # All procedure finished
    return


if __name__ == '__main__':
    # Enable Logging
    # logging.basicConfig(level=logging.DEBUG)
    # Define Database Details
    db_credential = ['../../connection-details/db-reco-engine.credential',
                     'reco-engine', 'test']
    # Execute main()
    main(db_cred=db_credential, worker_count=64)
    pass
