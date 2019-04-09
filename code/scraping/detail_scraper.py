from bs4 import BeautifulSoup
import urllib3
import certifi
from pprint import pprint


def dscraper(url):
    # Define output variable
    output = dict()
    # Full_URL
    furl = 'https://www.rottentomatoes.com{}'.format(url)
    output['Movie_URL'] = url
    # Load Page
    page = urllib3.PoolManager(cert_reqs='CERT_REQUIRED',
                               ca_certs=certifi.where()
                               ).request('GET', furl).data
    # Brew soup
    soup = BeautifulSoup(page, "html.parser")
    # Franchise/Flag
    output['Franchise'] = soup.select_one('div.franchiseLink').select_one('em').text.strip()
    # Movie_Name
    output['Movie_Name'] = soup.select_one(
        'h1.mop-ratings-wrap__title.mop-ratings-wrap__title--top').text.strip()
    # Critics_Consensus
    output['Critics_Consensus'] = soup.select_one(
        'p.mop-ratings-wrap__text.mop-ratings-wrap__text--concensus').text.strip()
    # TomatoMeter & AudienceScore
    for rate_panel in soup.select('div.mop-ratings-wrap__half'):
        if rate_panel.find(href='#contentReviews'):
            # print('Tomato!')
            output['Tomato_Meter'] = rate_panel.select_one('span.mop-ratings-wrap__percentage').text.strip()
            output['Tomato_Meter_rc'] = rate_panel.select_one('small.mop-ratings-wrap__text--small').text.strip()
        elif rate_panel.find(href='#audience_reviews'):
            # print('Audience!')
            output['Audience_Score'] = rate_panel.select_one('span.mop-ratings-wrap__percentage').text[:-9].strip()
            output['Audience_Score_ur'] = rate_panel.select_one('small.mop-ratings-wrap__text--small').text.strip()
        else:
            pass
    # Movie Info
    mi = soup.select_one('section.panel.panel-rt.panel-box.movie_info.media')
    # MI_Description
    output['MI_Description'] = mi.find(id='movieSynopsis').text.strip()
    for mi_item in mi.select('li.meta-row.clearfix'):
        mi_item_label = mi_item.select_one('div.meta-label.subtle').text.strip()[:-1]
        # MI_Rating
        if mi_item_label == 'Rating':
            output['MI_Rating'] = mi_item.select_one('div.meta-value').text.strip()
        # MI_Genre
        elif mi_item_label == 'Genre':
            output['MI_Genre'] = tuple(int(genre['href'][24:]) for genre in mi_item.select('a'))
        # MI_Directed_by
        elif mi_item_label == 'Directed By':
            output['MI_Director'] = tuple(director.text.strip() for director in mi_item.select('a'))
        # MI_Written_by
        elif mi_item_label == 'Written By':
            output['MI_Writer'] = tuple(writer.text.strip() for writer in mi_item.select('a'))
        # MI_In_Theaters
        elif mi_item_label == 'In Theaters':
            output['MI_In_Theaters_0'] = mi_item.select_one('time')['datetime']
            output['MI_In_Theaters_1'] = mi_item.select_one('time').text.strip()
        # MI_On_Disc/Streaming
        elif mi_item_label == 'On Disc/Streaming':
            output['MI_On_Disc_0'] = mi_item.select_one('time')['datetime']
            output['MI_On_Disc_1'] = mi_item.select_one('time').text.strip()
        # MI_Runtime
        elif mi_item_label == 'Runtime':
            output['MI_Runtime_0'] = mi_item.select_one('time')['datetime']
            output['MI_Runtime_1'] = mi_item.select_one('time').text.strip()
        # MI_Studio
        elif mi_item_label == 'Studio':
            output['MI_Studio'] = tuple(studio.text.strip() for studio in mi_item.select('a'))
        # Catch New Labels
        else:
            input('New MI label "{}". Please acknowledge:\n'.format(mi_item_label))
            break
    # Watch_It_Now/Streaming_Services
    output['Watch_It_Now'] = tuple(m_link.select_one('div.logo').text.strip()
                                   for m_link in soup.select_one('div.movie_links').select('a'))
    # Casts(Name+As)
    output['Casts'] = tuple(tuple([cast.select_one('div.media-body').select_one('span').text.strip(),
                                   cast.select_one('span.characters.subtle.smaller').text[3:].strip()])
                            for cast in soup.select_one('div.castSection').select('div.cast-item.media.inlineBlock'))
    return output

if __name__ == '__main__':
    pprint(dscraper('/m/star_wars_episode_iii_revenge_of_the_sith'))
