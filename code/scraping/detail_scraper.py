from get_existing_urls import get_existing_urls
from db_connect import db_connect
from bs4 import BeautifulSoup
import requests
import math
import read
import time


def lscrape(col_in_use):
    # Read dependency files
    initial_urls = read.by_line('../../dependencies/rt_initial_urls')
    genre_codes = read.by_line('../../dependencies/rt_genre_codes')
    # Load existing urls from db
    existing_urls = get_existing_urls(col_in_use=col_in_use)
    # Define other variables
    new_urls = list()
    # Scrape on
    for initial_url in initial_urls:
        for genre_code in genre_codes:
            page_num = 1
            # Knit url
            i_url = initial_url.format(genre=genre_code, page=1)
            # Learn page limit
            max_page = math.ceil(requests.get(i_url).json()['counts']['total'] / 32)
            # Go for launch!
            while True:
                if page_num <= max_page:
                    # Knit url, again
                    url = initial_url.format(genre=genre_code, page=page_num)
                    # Load page
                    data = requests.get(url).json()
                    print('\r\n{} on page | Page {}/{} | URL: {}'.format(
                        data['counts']['count'], page_num, max_page, url))
                    # Get movie_urls on page
                    for item in data['results']:
                        if item['url'] not in existing_urls + new_urls \
                                and item['url'] != '/m/null' and len(item['url']) > 1:
                            new_urls.append(item['url'])
                            print(item['url'])
                    # Add 1 to page counter
                    page_num += 1
                else:
                    break
            continue
        continue
    # Print number of movie_urls captured
    print('\r\n{} new movie(s) found\r\n'.format(len(new_urls)))
    return new_urls


def dscrape(url):
    # Define output variable
    output = dict()
    # Full_URL
    furl = 'https://www.rottentomatoes.com{}'.format(url)
    output['Movie_URL'] = url
    print('Obtaining detail page: {}'.format(furl))
    # Load Page (max 5 times)
    page = None
    for t in range(5):
        try:
            page = requests.get(furl)
        except requests.exceptions.ConnectionError:
            print('\t- Connection timed out. Try again')
        else:
            break
    if not page:  # Page load error
        print('\t- Bad page (0). Moving on')
        return None
    else:
        if page.status_code != 200:  # Page status code error
            print('\t- Bad page (1). Moving on')
            return None
        else:  # Page load successful
            # Brew soup
            soup = BeautifulSoup(page.content, "html.parser")
            # FIND Franchise/Flag
            try:
                output['Franchise'] = soup.select_one('div.franchiseLink').select_one('em').text.strip()
            except AttributeError:
                print('\t- Franchise not found')
            # FIND Movie_Name
            try:
                output['Movie_Name'] = soup.select_one(
                    'h1.mop-ratings-wrap__title.mop-ratings-wrap__title--top').text.strip()
            except AttributeError:
                print('\t- Movie_Name not found')
            # FIND Critics_Consensus
            try:
                output['Critics_Consensus'] = soup.select_one(
                    'p.mop-ratings-wrap__text.mop-ratings-wrap__text--concensus').text.strip()
            except AttributeError:
                print('\t- Critics_Consensus not found')
            # FIND TomatoMeter & AudienceScore
            try:
                rate_panels = soup.select('div.mop-ratings-wrap__half')
            except AttributeError:
                print('\t- Rate_Panels not found')
            else:
                for rate_panel in rate_panels:
                    if rate_panel.find(href='#contentReviews'):
                        # print('Tomato!')
                        try:
                            output['Tomato_Meter'] = rate_panel.select_one('span.mop-ratings-wrap__percentage').text.strip()
                        except AttributeError:
                            print('\t- Tomato_Meter not found')
                        try:
                            output['Tomato_Meter_rc'] = rate_panel.select_one('small.mop-ratings-wrap__text--small').text.strip()
                        except AttributeError:
                            print('\t- Tomato_Meter_rc not found')
                    elif rate_panel.find(href='#audience_reviews'):
                        # print('Audience!')
                        try:
                            output['Audience_Score'] = rate_panel.select_one('span.mop-ratings-wrap__percentage').text[:-9].strip()
                        except AttributeError:
                            print('\t- Audience_Score not found')
                        try:
                            output['Audience_Score_ur'] = rate_panel.select_one('small.mop-ratings-wrap__text--small').text.strip()
                        except AttributeError:
                            print('\t- Audience_Score_ur not found')
                    else:
                        pass
            # Movie Info
            try:
                mi = soup.select_one('section.panel.panel-rt.panel-box.movie_info.media')
            except AttributeError:
                print('\t- Movie_Info not found')
            else:
                # FIND MI_Description
                try:
                    output['MI_Description'] = mi.find(id='movieSynopsis').text.strip()
                except AttributeError:
                    print('\t- MI_Description not found')
                for mi_item in mi.select('li.meta-row.clearfix'):
                    try:
                        mi_item_label = mi_item.select_one('div.meta-label.subtle').text.strip()[:-1]
                    except AttributeError:
                        print('\t- Cannot read current MI_label')
                        continue
                    else:
                        # FIND MI_Rating
                        if mi_item_label == 'Rating':
                            try:
                                output['MI_Rating'] = mi_item.select_one('div.meta-value').text.strip()
                            except AttributeError:
                                print('\t\t- Bad MI_Rating')
                        # FIND MI_Genre
                        elif mi_item_label == 'Genre':
                            try:
                                output['MI_Genre'] = tuple(int(genre['href'][24:]) for genre in mi_item.select('a'))
                            except AttributeError:
                                print('\t\t- Bad MI_Genre')
                        # FIND MI_Directed_by
                        elif mi_item_label == 'Directed By':
                            try:
                                output['MI_Director'] = tuple(director.text.strip() for director in mi_item.select('a'))
                            except AttributeError:
                                try:
                                    output['MI_Director'] = mi_item.select_one('div.meta-value').text.strip()
                                except AttributeError:
                                    print('\t\t- Bad MI_Director')
                        # FIND MI_Written_by
                        elif mi_item_label == 'Written By':
                            try:
                                output['MI_Writer'] = tuple(writer.text.strip() for writer in mi_item.select('a'))
                            except AttributeError:
                                try:
                                    output['MI_Writer'] = mi_item.select_one('div.meta-value').text.strip()
                                except AttributeError:
                                    print('\t\t- Bad MI_Writer')
                        # FIND MI_In_Theaters
                        elif mi_item_label == 'In Theaters':
                            try:
                                output['MI_In_Theaters_0'] = mi_item.select_one('time')['datetime']
                                output['MI_In_Theaters_1'] = mi_item.select_one('time').text.strip()
                            except AttributeError:
                                print('\t\t- Bad MI_In_Theaters')
                        # FIND MI_On_Disc/Streaming
                        elif mi_item_label == 'On Disc/Streaming':
                            try:
                                output['MI_On_Disc_0'] = mi_item.select_one('time')['datetime']
                                output['MI_On_Disc_1'] = mi_item.select_one('time').text.strip()
                            except AttributeError:
                                print('\t\t- Bad MI_On_Disc')
                        # FIND MI_Runtime
                        elif mi_item_label == 'Runtime':
                            try:
                                output['MI_Runtime_0'] = mi_item.select_one('time')['datetime']
                                output['MI_Runtime_1'] = mi_item.select_one('time').text.strip()
                            except AttributeError:
                                print('\t\t- Bad MI_Runtime')
                        # FIND MI_Studio
                        elif mi_item_label == 'Studio':
                            try:
                                output['MI_Studio'] = mi_item.select_one('div.meta-value').text.strip()
                            except AttributeError:
                                print('\t\t- Bad MI_Studio')
                        # FIND MI_Box_Office
                        elif mi_item_label == 'Box Office':
                            try:
                                output['MI_Box_Office'] = mi_item.select_one('div.meta-value').text.strip()
                            except AttributeError:
                                print('\t\t- Bad MI_Box_Office')
                        # Catch New Labels
                        else:
                            input('New MI label "{}". Please acknowledge:\n'.format(mi_item_label))
            # FIND Watch_It_Now/Streaming_Services
            try:
                output['Watch_It_Now'] = tuple(m_link.select_one('div.logo').text.strip()
                                               for m_link in soup.select_one('div.movie_links').select('a'))
            except AttributeError:
                print('\t- Watch_It_Now not found')
            # FIND Casts(Name+As)
            try:
                output['Casts'] = tuple(tuple(
                    [cast.select_one('div.media-body').select_one('span').text.strip(),
                     cast.select_one('span.characters.subtle.smaller').text[3:].strip()])
                                        for cast in soup.select_one('div.castSection').select(
                    'div.cast-item.media.inlineBlock'))
            except AttributeError:
                print('\t- Casts not found')
            return output


if __name__ == '__main__':
    # Record start time
    start_time = time.time()
    # Start scraping
    new_URLs = lscrape(col_in_use='test')
    for new_URL in new_URLs:
        # Print progress
        print('\r\n(#{}/{}) '.format(new_URLs.index(new_URL) + 1, len(new_URLs)), end='')
        # Scrape detail page
        page_data = dscrape(new_URL)
        # Upload to db
        if page_data:
            db_connect(db_cred='../../connection-details/db-reco-engine.credential',
                       col_in_use='test').insert_one(page_data)
        continue
    # Print total run time
    print('\r\nRun time: {} seconds\r\n'.format(int(time.time() - start_time)))
