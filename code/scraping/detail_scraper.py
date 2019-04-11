from bs4 import BeautifulSoup
from pprint import pprint
import urllib3
import certifi


def dscraper(url):
    # Define output variable
    output = dict()
    # Full_URL
    furl = 'https://www.rottentomatoes.com{}'.format(url)
    output['Movie_URL'] = url
    print('\r\nObtaining detail page: {}'.format(furl))
    # Load Page
    page = urllib3.PoolManager(
        cert_reqs='CERT_REQUIRED', ca_certs=certifi.where()).request('GET', furl).data
    # Brew soup
    soup = BeautifulSoup(page, "html.parser")
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
    for rate_panel in soup.select('div.mop-ratings-wrap__half'):
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
                        print('\t\t- Bad MI_Director')
                # FIND MI_Written_by
                elif mi_item_label == 'Written By':
                    try:
                        output['MI_Writer'] = tuple(writer.text.strip() for writer in mi_item.select('a'))
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
                        output['MI_Studio'] = tuple(studio.text.strip() for studio in mi_item.select('a'))
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
                    # break
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
    data = dscraper('/m/charlie_and_the_chocolate_factory')
    print('\n')
    pprint(data)
