import sys
sys.path.append('../scraping')
sys.path.append('../tools')

import math
import read
import time
import requests
import db_connect
from bs4 import BeautifulSoup
from get_existing_urls import get_existing_urls
from multiprocessing import Pool


def get_page_limits(combination):
    # Split query inputs
    ini_url, gen_code = combination
    # Learn page limits for each query combination
    max_page = math.ceil(requests.get(ini_url.format(genre=gen_code, page=1)).json()['counts']['total'] / 32)
    # Return data as a key-value pair (url: max-page)
    return {ini_url.format(genre=gen_code, page='{}'): max_page}


def get_movie_urls(listing_url):
    # Define output list
    output = list()
    # Load page
    data = requests.get(listing_url).json()
    # Get movie urls on page
    for item in data['results']:
        if item['url'] != '/m/null' and len(item['url']) > 1:
            output.append(item['url'])
    # Return a list of movie urls
    return output


def get_movie_detail(url):
    # Define output variable
    output = dict()
    # Full_URL
    furl = 'https://www.rottentomatoes.com{}'.format(url)
    output['Movie_URL'] = url
    # print('Obtaining detail page: {}'.format(furl))
    # Load Page (max 5 times)
    page = None
    for t in range(5):
        try:
            page = requests.get(furl)
        except requests.exceptions.ConnectionError:
            # print('\t- Connection timed out. Try again')
            pass
        else:
            break
    if not page:  # Page load error
        print(f'\t- Bad page (0). Moving on | {furl}')
        return False
    else:
        if page.status_code != 200:  # Page status code error
            print(f'\t- Bad page (1). Moving on | {furl}')
            return False
        else:  # Page load successful
            # Brew soup
            soup = BeautifulSoup(page.content, "html.parser")
            # FIND Franchise/Flag
            try:
                output['Franchise'] = soup.select_one('div.franchiseLink').select_one('em').text.strip()
            except AttributeError:
                # print('\t- Franchise not found')
                pass
            # FIND Movie_Name
            try:
                output['Movie_Name'] = soup.select_one(
                    'h1.mop-ratings-wrap__title.mop-ratings-wrap__title--top').text.strip()
            except AttributeError:
                # print('\t- Movie_Name not found')
                pass
            # FIND Critics_Consensus
            try:
                output['Critics_Consensus'] = soup.select_one(
                    'p.mop-ratings-wrap__text.mop-ratings-wrap__text--concensus').text.strip()
            except AttributeError:
                # print('\t- Critics_Consensus not found')
                pass
            # FIND TomatoMeter & AudienceScore
            try:
                rate_panels = soup.select('div.mop-ratings-wrap__half')
            except AttributeError:
                # print('\t- Rate_Panels not found')
                pass
            else:
                for rate_panel in rate_panels:
                    if rate_panel.find(href='#contentReviews'):
                        # # print('Tomato!')
                        try:
                            output['Tomato_Meter'] = rate_panel.select_one('span.mop-ratings-wrap__percentage').text.strip()
                        except AttributeError:
                            # print('\t- Tomato_Meter not found')
                            pass
                        try:
                            output['Tomato_Meter_rc'] = rate_panel.select_one('small.mop-ratings-wrap__text--small').text.strip()
                        except AttributeError:
                            # print('\t- Tomato_Meter_rc not found')
                            pass
                    elif rate_panel.find(href='#audience_reviews'):
                        # # print('Audience!')
                        try:
                            output['Audience_Score'] = rate_panel.select_one('span.mop-ratings-wrap__percentage').text[:-9].strip()
                        except AttributeError:
                            # print('\t- Audience_Score not found')
                            pass
                        try:
                            output['Audience_Score_ur'] = rate_panel.select_one('small.mop-ratings-wrap__text--small').text.strip()
                        except AttributeError:
                            # print('\t- Audience_Score_ur not found')
                            pass
                    else:
                        pass
            # Movie Info
            try:
                mi = soup.select_one('section.panel.panel-rt.panel-box.movie_info.media')
            except AttributeError:
                # print('\t- Movie_Info not found')
                pass
            else:
                # FIND MI_Description
                try:
                    output['MI_Description'] = mi.find(id='movieSynopsis').text.strip()
                except AttributeError:
                    # print('\t- MI_Description not found')
                    pass
                for mi_item in mi.select('li.meta-row.clearfix'):
                    try:
                        mi_item_label = mi_item.select_one('div.meta-label.subtle').text.strip()[:-1]
                    except AttributeError:
                        # print('\t- Cannot read current MI_label')
                        pass
                        continue
                    else:
                        # FIND MI_Rating
                        if mi_item_label == 'Rating':
                            try:
                                output['MI_Rating'] = mi_item.select_one('div.meta-value').text.strip()
                            except AttributeError:
                                # print('\t\t- Bad MI_Rating')
                                pass
                        # FIND MI_Genre
                        elif mi_item_label == 'Genre':
                            try:
                                output['MI_Genre'] = tuple(int(genre['href'][24:]) for genre in mi_item.select('a'))
                            except AttributeError:
                                # print('\t\t- Bad MI_Genre')
                                pass
                        # FIND MI_Directed_by
                        elif mi_item_label == 'Directed By':
                            try:
                                output['MI_Director'] = tuple(director.text.strip() for director in mi_item.select('a'))
                            except AttributeError:
                                try:
                                    output['MI_Director'] = mi_item.select_one('div.meta-value').text.strip()
                                except AttributeError:
                                    # print('\t\t- Bad MI_Director')
                                    pass
                        # FIND MI_Written_by
                        elif mi_item_label == 'Written By':
                            try:
                                output['MI_Writer'] = tuple(writer.text.strip() for writer in mi_item.select('a'))
                            except AttributeError:
                                try:
                                    output['MI_Writer'] = mi_item.select_one('div.meta-value').text.strip()
                                except AttributeError:
                                    # print('\t\t- Bad MI_Writer')
                                    pass
                        # FIND MI_In_Theaters
                        elif mi_item_label == 'In Theaters':
                            try:
                                output['MI_In_Theaters_0'] = mi_item.select_one('time')['datetime']
                                output['MI_In_Theaters_1'] = mi_item.select_one('time').text.strip()
                            except AttributeError:
                                # print('\t\t- Bad MI_In_Theaters')
                                pass
                        # FIND MI_On_Disc/Streaming
                        elif mi_item_label == 'On Disc/Streaming':
                            try:
                                output['MI_On_Disc_0'] = mi_item.select_one('time')['datetime']
                                output['MI_On_Disc_1'] = mi_item.select_one('time').text.strip()
                            except AttributeError:
                                # print('\t\t- Bad MI_On_Disc')
                                pass
                        # FIND MI_Runtime
                        elif mi_item_label == 'Runtime':
                            try:
                                output['MI_Runtime_0'] = mi_item.select_one('time')['datetime']
                                output['MI_Runtime_1'] = mi_item.select_one('time').text.strip()
                            except AttributeError:
                                # print('\t\t- Bad MI_Runtime')
                                pass
                        # FIND MI_Studio
                        elif mi_item_label == 'Studio':
                            try:
                                output['MI_Studio'] = mi_item.select_one('div.meta-value').text.strip()
                            except AttributeError:
                                # print('\t\t- Bad MI_Studio')
                                pass
                        # FIND MI_Box_Office
                        elif mi_item_label == 'Box Office':
                            try:
                                output['MI_Box_Office'] = mi_item.select_one('div.meta-value').text.strip()
                            except AttributeError:
                                # print('\t\t- Bad MI_Box_Office')
                                pass
                        # Catch New Labels
                        else:
                            input('New MI label "{}". Please acknowledge:\n'.format(mi_item_label))
            # FIND Watch_It_Now/Streaming_Services
            try:
                output['Watch_It_Now'] = tuple(m_link.select_one('div.logo').text.strip()
                                               for m_link in soup.select_one('div.movie_links').select('a'))
            except AttributeError:
                # print('\t- Watch_It_Now not found')
                pass
            # FIND Casts(Name+As)
            try:
                output['Casts'] = tuple(tuple(
                    [cast.select_one('div.media-body').select_one('span').text.strip(),
                     cast.select_one('span.characters.subtle.smaller').text[3:].strip()])
                                        for cast in soup.select_one('div.castSection').select(
                    'div.cast-item.media.inlineBlock'))
            except AttributeError:
                # print('\t- Casts not found')
                pass
            # Return output as a dictionary for database insertion
            return output


def scrape_with_mp(initial_url_list, genre_code_list, db_cred, worker_count1=32, worker_count2=6):
    # Record start time
    start_time1 = time.time()
    # Print start status
    print('\r\nProcedure 1 started\r\n')

    # Procedure 0: Separate database connection details
    db_cred_fpath, db_in_use, col_in_use = db_cred
    existing_url_list = get_existing_urls(db_connect.get_collection(db_cred_fpath, db_in_use, col_in_use))

    # Procedure 1: Create query combinations
    combinations = list((initial_url, genre_code) for genre_code in genre_code_list
                        for initial_url in initial_url_list)
    listing_urls = dict()
    # Procedure 2: Get listing urls bases
    for result in Pool(worker_count1).map(get_page_limits, combinations):
        listing_urls.update(result)
    # Procedure 3: Brew all listing urls
    listing_urls = list(i_url.format(page_number + 1) for i_url in listing_urls
                        for page_number in range(listing_urls[i_url]))
    # Procedure 4: Grab movie urls from listing pages
    movie_urls = set(movie_url for lists in Pool(worker_count1).map(get_movie_urls, listing_urls)
                     for movie_url in lists if movie_url not in existing_url_list)
    print('{} urls obtained'.format(len(movie_urls)))
    # Print end status
    print('Procedure 1 finished')
    # Print runtime
    print('Runtime: {} seconds\r\n'.format(int(time.time() - start_time1)))

    # Record start time
    start_time2 = time.time()
    # Print start status
    print('\r\nProcedure 2 started\r\n')
    # Procedure 5: Scrape on!
    results = list(result for result in Pool(worker_count2).map(get_movie_detail, movie_urls) if result)
    # Procedure 6: Insert data into database
    if len(results) > 0:
        db_connect.get_collection(db_cred=db_cred_fpath, db=db_in_use, collection=col_in_use).insert_many(results)
    # Print insertion result
    print('{} inserted'.format(len(results)))
    # Delete results
    del results
    # Print end status
    print('Procedure 2 finished')
    # Print runtime
    print('Runtime: {} seconds\r\n'.format(int(time.time() - start_time2)))

    # All procedure finished
    return


if __name__ == '__main__':
    # Define global variables
    initial_urls = read.by_line('../../dependencies/rt_initial_urls')
    genre_codes = read.by_line('../../dependencies/rt_genre_codes')

    db_credential = ['../../connection-details/db-reco-engine.credential',
                     'reco-engine', 'test']

    # Start procedures
    scrape_with_mp(initial_url_list=initial_urls,
                   genre_code_list=genre_codes,
                   db_cred=db_credential,
                   worker_count1=48,
                   worker_count2=64)
